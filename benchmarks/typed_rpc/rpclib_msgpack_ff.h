// Benchmark adapter for rpclib v2.3.0's native MessagePack-RPC stream.
struct msgpack_reader {
    using message = mp::object_handle;
    mp::unpacker parser;
    bool read(int fd, message& object) {
        constexpr std::size_t chunk = 16 * 1024;
        while (!parser.next(object)) {
            parser.reserve_buffer(chunk);
            const auto n = read_some(fd, parser.buffer(), chunk);
            check(n >= 0, "MessagePack stream read failed");
            if (n == 0) {
                check(parser.message_size() == 0, "truncated MessagePack stream");
                return false;
            }
            parser.buffer_consumed(static_cast<std::size_t>(n));
        }
        return true;
    }
};

static void serve_msgpack(int fd) {
    rpc::detail::dispatcher dispatcher;
    dispatcher.bind("add", &add);
    dispatcher.bind("echo", &echo);
    msgpack_reader stream;
    mp::object_handle request;
    while (stream.read(fd, request)) {
        auto reply = dispatcher.dispatch(request.get(), true).get_data();
        transfer(fd, reinterpret_cast<std::uint8_t*>(reply.data()), reply.size(), true);
    }
}

// The stock packer visits the same tuple twice. The first sink only counts
// bytes; the second writes into the ring reservation, including wraparound.
struct msgpack_measure {
    w::measure measured{128 * 1024};
    void write(const char*, std::size_t n) { measured.add(n); }
};
struct msgpack_writer {
    w::writer& out;
    void write(const char* bytes, std::size_t n) { check(out.raw(bytes, n), "MessagePack reservation overflow"); }
};
template<class Tuple> class msgpack_request {
    Tuple fields_;
    msgpack_measure size_;
public:
    explicit msgpack_request(Tuple fields) : fields_(std::move(fields)) { mp::pack(size_, fields_); }
    std::size_t size() const { return size_.measured.size(); }
    w::errc error() const { return size_.measured.error(); }
    w::errc write(w::writer& out) const {
        msgpack_writer sink{out};
        mp::pack(sink, fields_);
        return out.empty() ? w::errc::ok : w::errc::invalid_value;
    }
};
using msgpack_io = ff_tcp_transport<msgpack_reader>;

template<class R, class... A>
static auto msgpack_call(std::shared_ptr<msgpack_io> io, std::uint32_t id, const char* name, A&&... args) {
    auto fields = std::make_tuple(0, id, std::string(name), std::make_tuple(std::forward<A>(args)...));
    msgpack_request<decltype(fields)> request(std::move(fields));
    return portal::make_invocation<R>(std::move(io),
        [id, request = std::move(request)](msgpack_io& transport, auto token) mutable {
            return transport.async_round_trip_into(std::move(request), [id, token](mp::object_handle obj) {
                try {
                    rpc::detail::response reply(std::move(obj));
                    if (reply.get_id() != id || reply.get_error()) {
                        token.finish(portal::async_result<R>(std::make_error_code(std::errc::protocol_error)));
                        return;
                    }
                    token.finish(portal::async_result<R>(reply.get_result()->get().template as<R>()));
                } catch (...) {
                    token.finish(portal::async_result<R>(std::current_exception()));
                }
            });
        });
}

static void msgpack_ff_suite(int fd, unsigned short port, const char* name, int round, int window, bool nodelay) {
    auto io = std::make_shared<msgpack_io>(fd);
    std::uint32_t id = 0;
    bool ready = false;
    msgpack_call<number>(io, ++id, "add", 2, 3).start([&](portal::async_result<number> r) noexcept {
        check(r.has_value() && *r.value == 5, "MessagePack warmup failed");
        ready = true;
    });
    while (!ready) io->progress();
    if (nodelay) enable_nodelay(port);
    ff_workload(name, round, window, 0, 4096, *io,
        [&](number i) { return msgpack_call<number>(io, ++id, "add", i, number(7)); },
        [](int i, number result) { return result == i + 7; });
    for (int size : {16, 1024, 65536}) {
        auto data = payloads(size, window);
        ff_workload(name, round, window, size, size == 65536 ? 512 : 2048, *io,
            [&](int i) { return msgpack_call<std::string>(io, ++id, "echo", data[i % window]); },
            [&](int i, const std::string& result) { return result == data[i % window]; });
    }
}

static int check_rpclib_ff() {
    // A stock client also consumes the replacement server's unframed stream.
    with_tcp_server(serve_msgpack, [](unsigned short port) {
        rpc::client client("127.0.0.1", port);
        client.set_timeout(5000);
        check(client.call("add", -19, 7).as<number>() == -12, "native client add failed");
        auto data = payloads(65536, 4);
        std::vector<decltype(client.async_call("echo", data.front()))> replies;
        for (const auto& x : data) replies.push_back(client.async_call("echo", x));
        for (std::size_t i = 0; i < data.size(); ++i)
            check(replies[i].get().as<std::string>() == data[i], "native client echo failed");
    });
    // Check the reverse direction, including a real remote error and a subsequent success.
    rpc::server server("127.0.0.1", 0);
    server.bind("add", &add);
    server.suppress_exceptions(true);
    server.async_run(1);
    with_client_socket(server.port(), [](int fd) {
        auto io = std::make_shared<msgpack_io>(fd);
        unsigned completed = 0;
        msgpack_call<number>(io, 81, "unknown").start([&](portal::async_result<number> r) noexcept {
            check(!r.has_value() && r.error.code == std::errc::protocol_error, "expected remote error");
            ++completed;
        });
        msgpack_call<number>(io, 82, "add", -19, 7).start([&](portal::async_result<number> r) noexcept {
            check(r.has_value() && *r.value == -12, "FF client add failed");
            ++completed;
        });
        while (completed < 2) io->progress();
    });
    return 0;
}
