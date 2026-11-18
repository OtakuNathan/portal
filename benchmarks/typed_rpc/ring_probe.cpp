// Time request preparation, submission, ring publication and consumption on one
// thread. Both transports return the same inline acknowledgement without a socket.
#include "benchmarks/rpc_ring.h"
#include "portal/rpc/typed_async.h"
#include <chrono>
#include <cstdio>

namespace rb = rpc_ring_bench;
namespace tr = portal::typed_rpc;
namespace w = portal::wire;
using send_blob = tr::method<1, 1, w::none, w::blob<131072>>;

struct transport {
    rb::byte_ring ring{262144, 262144};
    w::bytes expected, reply;

    explicit transport(const std::string& payload) {
        rb::check(tr::encode_request<send_blob>(expected, {}, payload) == w::errc::ok);
        rb::check(tr::encode_response<send_blob>(reply, {}) == w::errc::ok);
    }

    template<class H> portal::async_submission acknowledge(H done) {
        std::size_t offset = 0;
        while (ring.read([&](const std::uint8_t* data, std::size_t n) {
            rb::check(n <= expected.size() - offset && !std::memcmp(data, expected.data() + offset, n));
            offset += n;
            return n;
        })) {}
        rb::check(offset == expected.size());
        done(reply);
        return portal::async_submission::accept();
    }
    template<class H> portal::async_submission async_round_trip(w::bytes frame, H done) {
        rb::check(ring.try_write(frame.size(), [&](auto& out) { out.append(frame.data(), frame.size()); })
                  == rb::write_status::published);
        return acknowledge(std::move(done));
    }
    template<class H> portal::async_submission async_round_trip_into(tr::prepared_request<send_blob> request, H done) {
        rb::check(ring.try_write_typed(request) == rb::write_status::published);
        return acknowledge(std::move(done));
    }
};

template<class Call> void finish(Call&& call, bool erased, std::size_t& count) {
    auto done = [&](portal::async_result<w::unit> result) noexcept {
        rb::check(result.has_value());
        ++count;
    };
    if (erased) portal::erase_invocation(std::move(call)).start(done);
    else std::move(call).start(done);
}

static double run(const std::shared_ptr<transport>& t, const std::string& source,
                  bool direct, bool erased, std::size_t iterations) {
    std::size_t completed = 0;
    const auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < iterations; ++i) {
        std::string payload(source);
        if (direct) finish(tr::make_direct_rpc_invocation<send_blob>(t, {}, std::move(payload)), erased, completed);
        else finish(tr::make_rpc_invocation<send_blob>(t, {}, std::move(payload)), erased, completed);
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    rb::check(completed == iterations);
    return std::chrono::duration<double, std::nano>(elapsed).count() / iterations;
}

int main() {
    std::puts("payload_bytes,invocation,repeat,staged_ns,direct_ns");
    for (const std::size_t size : {16, 4096, 65536}) {
        const std::string source(size, 'x');
        auto t = std::make_shared<transport>(source);
        const auto iterations = std::max<std::size_t>(2500, 64000000 / (size + 512));
        for (const bool erased : {false, true}) {
            for (int repeat = -1; repeat < 7; ++repeat) {
                const bool direct_first = repeat % 2 == 0;
                const auto first = run(t, source, direct_first, erased, iterations);
                const auto second = run(t, source, !direct_first, erased, iterations);
                if (repeat >= 0)
                    std::printf("%zu,%s,%d,%.2f,%.2f\n", size, erased ? "erased" : "static", repeat,
                                direct_first ? second : first, direct_first ? first : second);
            }
        }
    }
}
