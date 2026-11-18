// Run one implementation and transport per process. Both sides use the same handlers.
#include <portal/extensions/flux_foundry/io_registration_registry.h>
#include <rpc/client.h>
#include <rpc/dispatcher.h>
#include <rpc/server.h>
#ifdef PORTAL_HAS_FLUX_FOUNDRY
#include <portal/rpc/typed_import.h>
#define PORTAL_IMPORT_DEF "benchmarks/typed_rpc/service_import.def"
#include <portal/import.h>
#undef PORTAL_IMPORT_DEF
#include "../rpc_ring_ff.h"
#include <portal/rpc/tcp_sender.h>
#include <fcntl.h>
#include <dirent.h>
#include <climits>
#include <netinet/tcp.h>
#include <deque>
#endif
#include <poll.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

namespace w = portal::wire;
namespace tr = portal::typed_rpc;
namespace mp = RPCLIB_MSGPACK;
using registry = portal::ff::io_registration_registry;
using number = std::int32_t;
using add_method = tr::method<1, 1, w::i32, w::i32, w::i32>;
using blob = w::blob<65536>;
using echo_method = tr::method<2, 1, blob, blob>;
using timer = std::chrono::steady_clock;

static void check(bool ok, const char* message) {
    if (!ok) { std::fprintf(stderr, "%s\n", message); std::abort(); }
}
static number add(number a, number b) { return a + b; }
static std::string echo(std::string input) { return input; }

static void setup(registry& service) {
    auto update = service.prepare({}, [](tr::router& r) {
        auto status = r.define<add_method>(&add);
        return status == w::errc::ok ? r.define<echo_method>(&echo) : status;
    });
    check(std::move(update).install() == registry::status::ok, "Portal registration failed");
}

template<class Call>
static void measure(const char* impl, const char* path, int round, std::size_t bytes, int count, Call call) {
    for (int i = 0; i < 64; ++i) call(i);
    std::vector<double> samples;
    samples.reserve(count);
    double total = 0;
    for (int i = 0; i < count; ++i) {
        auto start = timer::now();
        call(i);
        auto ns = std::chrono::duration<double, std::nano>(timer::now() - start).count();
        samples.push_back(ns);
        total += ns;
    }
    std::sort(samples.begin(), samples.end());
    std::printf("%s,%s,%d,%zu,%d,%.2f,%.0f,%.0f\n", impl, path, round, bytes, count, total / count,
        samples[(count - 1) / 2], samples[(99 * count + 99) / 100 - 1]);
}

template<class Add, class Echo>
static void suite(const char* impl, const char* path, int round, int scale, Add sum, Echo reflect) {
    measure(impl, path, round, 0, 4000 * scale, [&](number i) {
        check(sum(i, 7) == i + 7, "incorrect add result");
    });
    for (std::size_t size : {16u, 1024u, 65536u}) {
        std::string payload(size, '\0');
        for (std::size_t i = 0; i < size; ++i) payload[i] = static_cast<char>(i % 251);
        measure(impl, path, round, size, (size == 65536 ? 400 : 2000) * scale, [&](number i) {
            payload[0] = static_cast<char>(i % 251);
            check(reflect(payload) == payload, "incorrect echo result");
        });
    }
}

template<class Transport>
static void portal_calls(Transport& io, const char* path, int round, int scale) {
    tr::workspace space;
    suite("portal", path, round, scale, [&](number a, number b) {
        auto r = tr::call<add_method>(io, space, {}, a, b);
        check(bool(r), "add RPC error");
        return r.value;
    }, [&](const std::string& x) {
        auto r = tr::call<echo_method>(io, space, {}, x);
        check(bool(r), "echo RPC error");
        return std::move(r.value);
    });
}

struct local_transport {
    registry service;
    void round_trip_into(const w::bytes& request, w::bytes& response) {
        check(service.dispatch(request, response) == w::errc::ok, "dispatch error");
    }
};

// Blocking one-request transport for the benchmark. Send the native typed frame
// in one write and read its 32-byte header followed by the declared body.
static short socket_ready(int fd, short events) {
    pollfd p{fd, events, 0};
    int result;
    do { result = ::poll(&p, 1, 5000); } while (result < 0 && errno == EINTR);
    check(result == 1 && !(p.revents & (POLLERR | POLLNVAL)), "socket poll failed or timed out");
    return p.revents;
}
static ssize_t read_some(int fd, void* data, std::size_t size) {
    for (;;) {
        auto n = ::recv(fd, data, size, 0);
        if (n >= 0) return n;
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) return n;
        socket_ready(fd, POLLIN);
    }
}
static void transfer(int fd, std::uint8_t* data, std::size_t size, bool write) {
    while (size) {
        auto n = write ? ::send(fd, data, size, 0) : read_some(fd, data, size);
        if (n < 0 && errno == EINTR) continue;
        check(n > 0, "socket transfer failed");
        size -= n;
        data += n;
    }
}
static void send_frame(int fd, const w::bytes& data) {
    transfer(fd, const_cast<std::uint8_t*>(data.data()), data.size(), true);
}
static bool receive_frame(int fd, w::bytes& frame) {
    std::uint8_t header[tr::header_size];
    const auto n = read_some(fd, header, sizeof(header));
    if (n == 0) return false;
    check(n > 0, "header read failed");
    transfer(fd, header + n, sizeof(header) - n, false);
    w::reader reader(w::view(header + 20, 4), {});
    std::uint32_t size = 0;
    check(reader.integer(size) && size <= 65536 + 4, "invalid frame length");
    frame.resize(sizeof(header) + size);
    std::memcpy(frame.data(), header, sizeof(header));
    transfer(fd, frame.data() + sizeof(header), size, false);
    return true;
}
struct tcp_transport {
    int fd;
    void round_trip_into(const w::bytes& request, w::bytes& reply) {
        send_frame(fd, request);
        check(receive_frame(fd, reply), "premature EOF");
    }
};

template<class Run> static void with_client_socket(unsigned short port, Run run) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    int client = ::socket(AF_INET, SOCK_STREAM, 0);
    check(client >= 0 && ::connect(client, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0, "connect failed");
    run(client);
    ::close(client);
}

template<class Serve, class Run> static void with_tcp_server(Serve serve, Run run) {
    int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    check(listener >= 0 && ::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 &&
          ::listen(listener, 1) == 0, "TCP listen failed");
    socklen_t length = sizeof(addr);
    check(::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &length) == 0, "getsockname failed");
    std::thread server([&] {
        int peer = ::accept(listener, nullptr, nullptr);
        check(peer >= 0, "accept failed");
        ::close(listener);
        serve(peer);
        ::close(peer);
    });
    run(ntohs(addr.sin_port));
    server.join();
}

template<class Run> static void with_portal_tcp(Run run) {
    with_tcp_server([](int peer) {
        registry service;
        setup(service);
        w::bytes request, reply;
        while (receive_frame(peer, request)) {
            check(service.dispatch(request, reply) == w::errc::ok, "TCP dispatch error");
            send_frame(peer, reply);
        }
    }, [&](unsigned short port) { with_client_socket(port, run); });
}

struct local_msgpack {
    rpc::detail::dispatcher dispatcher;
    mp::sbuffer request;
    std::uint32_t id = 0;
    template<class R, class... A> R call(const std::string& method, const A&... args) {
        request.clear();
        mp::pack(request, std::make_tuple(0, ++id, std::cref(method), std::tie(args...)));
        auto message = mp::unpack(request.data(), request.size());
        auto reply = dispatcher.dispatch(message.get()).get_data();
        rpc::detail::response result(mp::unpack(reply.data(), reply.size()));
        check(result.get_id() == id && !result.get_error(), "MessagePack-RPC response error");
        return result.get_result()->get().as<R>();
    }
};
#ifdef PORTAL_HAS_FLUX_FOUNDRY
#include "rpclib_ff_comparison.h"
#endif

int main(int argc, char** argv) {
#ifdef PORTAL_HAS_FLUX_FOUNDRY
    if (argc == 2 && std::string(argv[1]) == "--check-rpclib-ff") return check_rpclib_ff();
    if (argc == 5) {
        const std::string path = argv[2];
        if (path == "pipeline" || path == "pipeline-nodelay")
            return compare_pipeline(argv[1], std::stoi(argv[3]), std::stoi(argv[4]), path == "pipeline-nodelay");
    }
#endif
    if (argc != 4) return 2;
    const std::string impl = argv[1], path = argv[2];
    int round = std::stoi(argv[3]);
    if (impl == "portal" && path == "tcp") with_portal_tcp([&](int fd) {
        tcp_transport io{fd};
        portal_calls(io, "tcp", round, 1);
    });
    else if (impl == "portal" && path == "local") {
        local_transport io;
        setup(io.service);
        portal_calls(io, "local", round, 10);
    } else if (impl == "rpclib" && path == "tcp") {
        rpc::server server("127.0.0.1", 0);
        server.bind("add", &add);
        server.bind("echo", &echo);
        server.async_run(1);
        rpc::client client("127.0.0.1", server.port());
        client.set_timeout(5000);
        suite("rpclib", "tcp", round, 1, [&](number a, number b) { return client.call("add", a, b).as<number>(); },
              [&](const std::string& x) { return client.call("echo", x).as<std::string>(); });
    } else if (impl == "rpclib" && path == "local") {
        local_msgpack local;
        local.dispatcher.bind("add", &add);
        local.dispatcher.bind("echo", &echo);
        suite("rpclib", "local", round, 10, [&](number a, number b) { return local.call<number>("add", a, b); },
              [&](const std::string& x) { return local.call<std::string>("echo", x); });
    } else return 2;
}
