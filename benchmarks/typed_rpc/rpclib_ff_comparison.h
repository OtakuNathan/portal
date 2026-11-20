// Additional lanes for rpclib_comparison.cpp. All client-side FF work has one I/O owner.
namespace rb = rpc_ring_bench;
namespace ff = flux_foundry;

static auto space_flow() {
    return ff::make_blueprint<rb::space_request, portal::async_error>()
        | ff::await<rb::ring_space_awaitable>() | ff::end();
}
struct typed_reply_reader {
    using message = w::bytes;
    bool read(int fd, message& frame) { return receive_frame(fd, frame); }
};
template<class Reader> class ff_tcp_transport {
    using message = typename Reader::message;
    Reader reader_;
    int fd_;
    static constexpr std::size_t capacity = 128 * 1024;
    rb::byte_ring ring_{capacity, 16 * 1024};
    portal::rpc::tcp_sender sender_;
    decltype(space_flow()) space_ = space_flow();
    // The single-worker benchmark server writes exactly one reply per request in order.
    std::deque<std::function<void(message)>> replies_;

    void read_reply() {
        check(!replies_.empty(), "unexpected reply");
        message frame;
        check(reader_.read(fd_, frame), "EOF before reply");
        auto done = std::move(replies_.front());
        replies_.pop_front();
        done(std::move(frame));
    }
public:
    std::size_t waits = 0, resumes = 0;
    explicit ff_tcp_transport(int fd) : fd_(fd), sender_(fd, 0) {
        const int flags = ::fcntl(fd, F_GETFL);
        check(flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0, "nonblocking socket failed");
    }
    bool waiting() const { return ring_.producer_paused(); }
    template<class Request, class Handler>
    portal::async_submission async_round_trip_into(Request request, Handler done) {
        check(!waiting(), "two producers waiting on one ring");
        replies_.emplace_back(std::move(done));
        // All publication callbacks run on this thread, including the one from ring_.release().
        const auto before = waits;
        try {
            rb::async_write_typed(ring_, space_, std::move(request), [this, before](portal::async_error e) noexcept {
                check(!e.code && !e.exception, "ring publication failed");
                if (waits != before) ++resumes;
            });
        } catch (...) {
            replies_.pop_back();
            throw;
        }
        if (waiting()) ++waits;
        return portal::async_submission::accept();
    }
    void progress() {
        const auto spans = ring_.peek(0, capacity);
        if (spans.first.size) {
            auto sent = sender_.send(spans.first, spans.second);
            if (!sent.error) {
                check(sent.accepted && sent.released == sent.accepted, "copied send failed");
                ring_.release(sent.released);
                return;
            }
            check(sent.error.value() == EAGAIN || sent.error.value() == EWOULDBLOCK, "TCP send failed");
            if (!(socket_ready(fd_, POLLIN | POLLOUT) & POLLIN)) return;
        }
        read_reply();
    }
};

struct wait_counts { std::size_t waits = 0, resumes = 0; };
template<class Batch>
static void measure_batches(const char* name, int round, int window, int size, int count, Batch run) {
    for (int i = 0; i < 64; i += window) run(i, window);
    std::vector<double> batch_times;
    batch_times.reserve((count + window - 1) / window);
    wait_counts total;
    const auto start = timer::now();
    for (int i = 0; i < count; i += window) {
        auto begin = timer::now();
        auto pressure = run(i, std::min(window, count - i));
        batch_times.push_back(std::chrono::duration<double, std::nano>(timer::now() - begin).count());
        total.waits += pressure.waits;
        total.resumes += pressure.resumes;
    }
    const auto seconds = std::chrono::duration<double>(timer::now() - start).count();
    check(total.waits == total.resumes, "missing FF resumption");
    std::sort(batch_times.begin(), batch_times.end());
    std::printf("%s,%d,%d,%d,%d,%.2f,%.0f,%.0f,%zu,%zu\n", name, round, window, size, count, count / seconds,
                batch_times[(batch_times.size()-1)/2], batch_times[(99*batch_times.size()+99)/100-1],
                total.waits, total.resumes);
}

template<class R, class Verify> struct verified_reply {
    using value_type = ff::result_t<R, portal::async_error>;
    Verify* verify;
    int id;
    int* completed;
    void emplace(value_type&& value) noexcept {
        check(value.has_value() && (*verify)(id, value.value()), "incorrect FF reply");
        ++*completed;
    }
};

template<class Transport, class MakeCall, class Verify>
static void ff_workload(const char* name, int round, int window, int size, int count,
                        Transport& io, MakeCall make_call, Verify verify) {
    using call = decltype(make_call(0));
    auto flow = ff::make_blueprint<call, portal::async_error>() | portal::ff::await_static_invocation<call>() | ff::end();
    measure_batches(name, round, window, size, count, [&](int base, int n) {
        int issued = 0, completed = 0;
        const auto waits = io.waits, resumes = io.resumes;
        while (completed < n) {
            while (issued < n && !io.waiting()) {
                int id = base + issued++;
                using receiver = verified_reply<typename call::result_type, Verify>;
                auto runner = ff::make_fast_runner_view(flow, receiver{&verify, id, &completed});
                runner(make_call(id));
            }
            io.progress();
        }
        return wait_counts{io.waits - waits, io.resumes - resumes};
    });
}

template<class Submit, class Verify>
static void rpclib_workload(const char* name, int round, int window, int size, int count, Submit submit, Verify verify) {
    std::vector<decltype(submit(0))> futures;
    futures.reserve(window);
    measure_batches(name, round, window, size, count, [&](int base, int n) {
        for (int i = 0; i < n; ++i) futures.push_back(submit(base+i));
        for (int i = 0; i < n; ++i) check(verify(base+i, futures[i].get()), "incorrect rpclib async reply");
        futures.clear();
        return wait_counts{};
    });
}

static std::vector<std::string> payloads(int size, int window) {
    std::vector<std::string> data(window, std::string(size, '\0'));
    for (int i = 0; i < window; ++i)
        for (int j = 0; j < size; ++j) data[i][j] = static_cast<char>((j + i) % 251);
    return data;
}
using bench_symbols = portal::import_symbols::bench_rpc_service;
template<class Symbol, class Signature> struct bench_routes;
template<> struct bench_routes<bench_symbols::add, add_method::signature_t> { using type = add_method; };
template<> struct bench_routes<bench_symbols::echo, std::string(const std::string&)> { using type = echo_method; };

template<class Add, class Echo>
static void ff_suite(ff_tcp_transport<typed_reply_reader>& io, const char* name, int round, int window,
                     Add add_call, Echo echo_call) {
    ff_workload(name, round, window, 0, 4096, io,
        [&](number i) { return add_call(i, number(7)); },
        [](int id, number value) { return value == id+7; });
    for (int size : {16, 1024, 65536}) {
        auto data = payloads(size, window);
        ff_workload(name, round, window, size, size == 65536 ? 512 : 2048, io,
            [&](int id) { return echo_call(data[id % window]); },
            [&](int id, const std::string& value) { return data[id % window] == value; });
    }
}

template<class Client>
static void ff_named_suite(ff_tcp_transport<typed_reply_reader>& io, const char* name, int round, int window,
                           const Client& client) {
    ff_suite(io, name, round, window, [&](number a, number b) { return client.add(a, b); },
             [&](const std::string& x) { return client.echo(x); });
}
// rpclib has no public socket accessor. For the diagnostic only, locate both
// connected endpoints of this process's private loopback server by its ephemeral port.
static void enable_nodelay(unsigned short port) {
#ifdef __linux__
    DIR* fds = ::opendir("/proc/self/fd");
#else
    DIR* fds = ::opendir("/dev/fd");
#endif
    check(fds != nullptr, "opening process fd directory failed");
    int matched = 0;
    while (auto* entry = ::readdir(fds)) {
        char* end = nullptr;
        const auto fd = std::strtol(entry->d_name, &end, 10);
        if (*end || fd < 0 || fd > INT_MAX) continue;
        sockaddr_in local{}, peer{};
        socklen_t length = sizeof(peer);
        if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &length) || peer.sin_family != AF_INET) continue;
        length = sizeof(local);
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &length)) continue;
        if (peer.sin_addr.s_addr != htonl(INADDR_LOOPBACK) || local.sin_addr.s_addr != htonl(INADDR_LOOPBACK) ||
            (ntohs(local.sin_port) != port && ntohs(peer.sin_port) != port)) continue;
        const int on = 1;
        check(::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on)) == 0, "TCP_NODELAY failed");
        ++matched;
    }
    ::closedir(fds);
    check(matched == 2, "expected both loopback endpoints");
}
#include "rpclib_msgpack_ff.h"

static int compare_pipeline(const std::string& impl, int round, int window, bool nodelay) {
    if (window != 1 && window != 32) return 2;
    if (impl == "ff" || impl == "ff-erased" || impl == "ff-named" || impl == "ff-named-erased") {
        with_portal_tcp([&](int fd) {
            if (nodelay) {
                sockaddr_in peer{};
                socklen_t length = sizeof(peer);
                check(::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &length) == 0, "peer port unavailable");
                tcp_transport warmup{fd};
                tr::workspace storage;
                auto sum = tr::call<add_method>(warmup, storage, {}, number(2), number(3));
                check(sum && sum.value == 5, "connection warmup failed");
                enable_nodelay(ntohs(peer.sin_port));
            }
            using io_type = ff_tcp_transport<typed_reply_reader>;
            auto io = std::make_shared<io_type>(fd);
            if (impl == "ff-named" || impl == "ff-named-erased") {
                using context = tr::direct_import_backend<io_type, bench_routes>::context;
                auto owner = std::make_shared<context>(io);
                using service = portal::bench_rpc_service<context>;
                if (impl == "ff-named")
                    ff_named_suite(*io, impl.c_str(), round, window,
                        portal::bind_async<portal::bench_rpc_service>(owner, service::object_t{}));
                else
                    ff_named_suite(*io, impl.c_str(), round, window,
                        portal::bind_erased_async<portal::bench_rpc_service>(owner, service::object_t{}));
            } else {
                auto run = [&](auto wrap) {
                    ff_suite(*io, impl.c_str(), round, window,
                        [&](number a, number b) { return wrap(tr::make_direct_rpc_invocation<add_method>(io, {}, a, b)); },
                        [&](const std::string& x) { return wrap(tr::make_direct_rpc_invocation<echo_method>(io, {}, x)); });
                };
                if (impl == "ff") run([](auto call) { return call; });
                else run([](auto call) { return portal::erase_invocation(std::move(call)); });
            }
        });
    } else if (impl == "rpclib-ff" || impl == "rpclib-ff-client") {
        auto run = [&](unsigned short port) {
            with_client_socket(port, [&](int fd) {
                // One warmup request/reply also ensures the native server has accepted the socket.
                msgpack_ff_suite(fd, port, impl.c_str(), round, window, nodelay);
            });
        };
        if (impl == "rpclib-ff") with_tcp_server(serve_msgpack, run);
        else {
            rpc::server server("127.0.0.1", 0);
            server.bind("add", &add);
            server.bind("echo", &echo);
            server.async_run(1);
            run(server.port());
        }
    } else if (impl == "rpclib") {
        rpc::server server("127.0.0.1", 0);
        server.bind("add", &add);
        server.bind("echo", &echo);
        server.async_run(1);
        rpc::client client("127.0.0.1", server.port());
        client.set_timeout(5000);
        if (nodelay) {
            check(client.call("add", number(1), number(2)).as<number>() == 3, "connection warmup failed");
            enable_nodelay(server.port());
        }
        rpclib_workload("rpclib", round, window, 0, 4096,
            [&](number i) { return client.async_call("add", i, number(7)); },
            [](int id, mp::object_handle&& r) { return r.as<number>() == id+7; });
        for (int size : {16, 1024, 65536}) {
            auto data = payloads(size, window);
            rpclib_workload("rpclib", round, window, size, size == 65536 ? 512 : 2048,
                [&](int id) { return client.async_call("echo", data[id % window]); },
                [&](int id, mp::object_handle&& r) { return r.as<std::string>() == data[id % window]; });
        }
    } else return 2;
    return 0;
}
