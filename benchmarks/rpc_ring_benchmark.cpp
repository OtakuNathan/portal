// Single producer and consumer. Compare the same wire fields and queue with
// and without an intermediate, preallocated frame vector.
#include "rpc_ring.h"
#ifdef PORTAL_HAS_FLUX_FOUNDRY
#include "rpc_ring_ff.h"
#endif

#ifdef PORTAL_COUNT_ALLOCATIONS
#include "reuse_pass/allocation_counter.h"
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <utility>

namespace {
using namespace rpc_ring_bench;

void check_boundaries() {
    byte_ring ring(32, 32);
    bytes source(32);
    for (std::size_t i = 0; i < source.size(); ++i) source[i] = static_cast<std::uint8_t>(i);
    auto fill = [&](auto& out) { out.append(source.data(), source.size()); };
    auto unexpected = [](auto&) { check(false); };
#if PORTAL_HAS_EXCEPTIONS
    struct abandoned {};
    try {
        ring.try_write(5, [](auto& out) { out.put_byte(17); throw abandoned{}; });
        check(false);
    } catch (const abandoned&) {}
    check(ring.read([](const std::uint8_t*, std::size_t) { check(false); return 0; }) == 0);
#endif
    check(ring.try_write(33, unexpected) == write_status::oversized);
    check(ring.try_write(32, fill) == write_status::published);
    check(ring.try_write(1, unexpected) == write_status::full);
    check(ring.read([](const std::uint8_t*, std::size_t) { return 0; }) == 0);
    check(ring.try_write(1, unexpected) == write_status::full);
    check(ring.read([&](const std::uint8_t* data, std::size_t n) {
        check(n == 32 && std::memcmp(data, source.data(), n) == 0);
        return 3;
    }) == 3);
    check(ring.try_write(3, [&](auto& out) { out.append(source.data(), 3); }) == write_status::published);
    check(ring.read([&](const std::uint8_t* data, std::size_t n) {
        check(n == 29 && std::memcmp(data, source.data() + 3, n) == 0);
        return n;
    }) == 29);
    check(ring.read([&](const std::uint8_t* data, std::size_t n) {
        check(n == 3 && std::memcmp(data, source.data(), n) == 0);
        return n;
    }) == 3);
}

void check_backpressure() {
    byte_ring ring(64, 16);
    bytes data(64, 42);
    auto send = [&](std::size_t n) {
        return ring.try_write(n, [&](auto& out) { out.append(data.data(), n); });
    };
    auto drain = [&](std::size_t n) {
        check(ring.read([&](const std::uint8_t*, std::size_t span) {
            check(n <= span);
            return n;
        }) == n);
    };
    auto check_resume = [&](std::size_t pending, std::size_t first, std::size_t next) {
        std::thread producer([&] {
            check(ring.write(pending, [&](auto& out) { out.append(data.data(), pending); })
                == write_status::published);
        });
        while (!ring.producer_paused()) std::this_thread::yield();
        drain(first);
        check(ring.producer_paused());
        drain(next);
        producer.join();
        check(!ring.producer_paused());
    };

    check(send(16) == write_status::published);
    check(send(1) == write_status::full);
    check_resume(1, 7, 1); // Nine bytes still queued, then eight (low water).
    drain(9);

    check(send(1) == write_status::published);
    check(send(40) == write_status::full);
    check_resume(40, 0, 1); // Above high water: wait for an empty ring.
    check(send(1) == write_status::full);
    drain(32); // Eight bytes of the large frame remain.
    check(send(8) == write_status::published);
    check(ring.write(65, [](auto&) { check(false); }) == write_status::oversized);
}

void check_wrapped_frames() {
    const auto text = value::string(std::string("a\0b", 3));
    const auto number = value::signed_integer(-123);
    const auto expected = codec::encode_request_values(123, text, number);
    for (std::size_t start = 0; start < 64; ++start) {
        byte_ring ring(64, 64);
        bytes padding(start);
        check(ring.try_write(start, [&](auto& out) { out.append(padding.data(), start); })
            == write_status::published);
        check(ring.read([](const std::uint8_t*, std::size_t n) { return n; }) == start);
        check(ring.try_write(expected.size(), [&](auto& out) {
            out.request_header(123, 2);
            codec::encode_value(out, text);
            codec::encode_value(out, number);
        }) == write_status::published);
        std::size_t offset = 0;
        while (offset != expected.size()) {
            const auto used = ring.read([&](const std::uint8_t* data, std::size_t n) {
                const auto part = std::min<std::size_t>(n, 3);
                check(part <= expected.size() - offset);
                check(std::memcmp(data, expected.data() + offset, part) == 0);
                return part;
            });
            check(used != 0);
            offset += used;
        }
    }
}

#ifdef PORTAL_HAS_FLUX_FOUNDRY
struct wait_observation {
    std::atomic<unsigned> calls{0};
    std::error_code error;
};

struct observe_wait {
    using value_type = space_result;
    wait_observation* observation;
    void emplace(space_result&& result) noexcept {
        check(observation->calls.fetch_add(1) == 0);
        if (!result.has_value()) observation->error = result.error().code;
    }
};

void check_ff_waits() {
    auto blueprint = ff::make_blueprint<space_request, portal::async_error>()
        | ff::await<ring_space_awaitable>() | ff::end();
    auto start = [&](byte_ring& ring, std::size_t size, wait_observation& observation) {
        auto runner = ff::make_fast_runner_view(blueprint, observe_wait{&observation});
        runner(space_request{&ring, size});
    };
    auto fill = [](auto& out) {
        const std::uint8_t frame[16]{};
        out.append(frame, sizeof(frame));
    };
    byte_ring ring(32, 16);
    wait_observation ready, deferred, busy, oversized, stopped;
    start(ring, 1, ready);
    check(ready.calls == 1 && !ready.error);
    start(ring, 33, oversized);
    check(oversized.calls == 1 && oversized.error == std::errc::message_size);
    check(ring.try_write(16, fill) == write_status::published);
    start(ring, 1, deferred);
    start(ring, 1, busy);
    check(deferred.calls == 0 && busy.calls == 1 && busy.error == std::errc::operation_in_progress);
    check(ring.read([](const std::uint8_t*, std::size_t) { return 7; }) == 7);
    check(deferred.calls == 0);
    check(ring.read([](const std::uint8_t*, std::size_t) { return 1; }) == 1);
    check(deferred.calls == 1 && !deferred.error);
    // A frame larger than high water must wait for an empty ring.
    start(ring, 32, stopped);
    check(stopped.calls == 0);
    ring.stop_async_waits();
    check(stopped.calls == 1 && stopped.error == std::errc::operation_canceled);
    ring.read([](const std::uint8_t*, std::size_t n) { return n; });
    ring.stop_async_waits();
    check(deferred.calls == 1 && stopped.calls == 1);
    wait_observation after_stop;
    start(ring, 1, after_stop);
    check(after_stop.calls == 1 && after_stop.error == std::errc::operation_canceled);

    // Stop and low water compete for the same backend reference.
    for (unsigned i = 0; i < 64; ++i) {
        byte_ring racing(32, 16);
        check(racing.try_write(16, fill) == write_status::published);
        wait_observation terminal;
        start(racing, 1, terminal);
        std::atomic<bool> go{false};
        std::thread reader([&] {
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            racing.read([](const std::uint8_t*, std::size_t n) { return n; });
        });
        go.store(true, std::memory_order_release);
        racing.stop_async_waits();
        reader.join();
        check(terminal.calls == 1 && (!terminal.error || terminal.error == std::errc::operation_canceled));
    }
}
#endif

struct fixture {
    std::array<value, 2> arguments;
    std::array<bytes, 2> expected;
    explicit fixture(std::size_t payload) {
        for (unsigned i = 0; i < 2; ++i) {
            if (payload) {
                std::string text(payload, '\0');
                for (std::size_t j = 0; j < payload; ++j) text[j] = static_cast<char>((j + i) % 251);
                arguments[i] = value::string(std::move(text));
            } else {
                arguments[i] = value::signed_integer(42 + i);
            }
            expected[i] = codec::encode_request_values(100 + i, arguments[i]);
        }
    }
};

template<bool Direct, bool Async = false>
double run(const fixture& frames, std::size_t count, std::size_t high_water) {
    byte_ring ring(1024 * 1024, high_water);
    bytes scratch;
    scratch.reserve(frames.expected[0].size());
    std::atomic<bool> ready{false};
    std::thread consumer([&] {
        std::size_t frame = 0, offset = 0;
        ready.store(true, std::memory_order_release);
        while (frame < count) {
            const auto used = ring.read([&](const std::uint8_t* data, std::size_t size) {
                // Model short writes. Verify every accepted byte at the receiver.
                const auto accepted = std::min<std::size_t>(size, 4096);
                std::size_t checked = 0;
                while (checked < accepted) {
                    const auto& expected = frames.expected[frame % 2];
                    const auto part = std::min(accepted - checked, expected.size() - offset);
                    check(std::memcmp(data + checked, expected.data() + offset, part) == 0);
                    offset += part;
                    checked += part;
                    if (offset == expected.size()) { offset = 0; ++frame; }
                }
                return accepted;
            });
            if (!used) std::this_thread::yield();
        }
    });
    while (!ready.load(std::memory_order_acquire)) std::this_thread::yield();
    const auto start = std::chrono::steady_clock::now();
#ifdef PORTAL_COUNT_ALLOCATIONS
    allocations = 0;
    count_enabled = true;
#endif
    auto submit_frame = [&](std::size_t i) {
        const auto& arg = frames.arguments[i % 2];
        const auto method = 100 + i % 2;
        const auto size = codec::checked_add(17, codec::encoded_size(arg));
        auto fill = [&](auto& out) {
            auto encode = [&](auto& writer) {
                writer.request_header(method, 1);
                codec::encode_value(writer, arg);
            };
            if (Direct) {
                encode(out);
            } else {
                codec::writer writer(std::move(scratch), size);
                encode(writer);
                scratch = writer.finish();
                out.append(scratch.data(), scratch.size());
            }
        };
        const auto status = Async ? ring.try_write(size, fill) : ring.write(size, fill);
        return std::make_pair(status, size);
    };
#ifdef PORTAL_HAS_FLUX_FOUNDRY
    if (Async) {
        run_ff_producer(ring, count, submit_frame);
    } else
#endif
    {
        for (std::size_t i = 0; i < count; ++i)
            check(submit_frame(i).first == write_status::published);
    }
#ifdef PORTAL_COUNT_ALLOCATIONS
    count_enabled = false;
    std::fprintf(stderr, "producer allocations for %zu frames: %zu\n", count, allocations);
#endif
    consumer.join();
    return std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count() / count;
}
}

int main(int argc, char** argv) {
    if (argc == 2 && std::strcmp(argv[1], "--check") == 0) {
        check_boundaries();
        check_wrapped_frames();
        check_backpressure();
        const fixture frames(1024);
        run<true>(frames, 5000, 4096);
        run<false>(frames, 5000, 4096);
#ifdef PORTAL_HAS_FLUX_FOUNDRY
        check_ff_waits();
        run<true, true>(frames, 5000, 4096);
#endif
        return 0;
    }
    const auto usage = [] {
        std::fputs("usage: rpc_ring_benchmark direct|staged"
#ifdef PORTAL_HAS_FLUX_FOUNDRY
            "|ff"
#endif
            " scalar|1024|65536 [high-water-KiB: 1..1024, default 256]\n", stderr);
        return 2;
    };
    if (argc != 3 && argc != 4) return usage();
    const std::string mode = argv[1];
    auto measure = &run<true>;
    if (mode == "staged") measure = &run<false>;
#ifdef PORTAL_HAS_FLUX_FOUNDRY
    else if (mode == "ff") measure = &run<true, true>;
#endif
    else if (mode != "direct") return usage();
    const std::string sample = argv[2];
    if (sample != "scalar" && sample != "1024" && sample != "65536") return 2;
    const std::size_t size = sample == "scalar" ? 0 : std::stoul(sample);
    const std::size_t count = !size ? 500000 : size == 1024 ? 100000 : 4000;
    std::size_t high_water_kib = 256;
    if (argc == 4) {
        char* end = nullptr;
        const auto parsed = std::strtoul(argv[3], &end, 10);
        if (*end || parsed < 1 || parsed > 1024) return 2;
        high_water_kib = parsed;
    }
    const auto high_water = high_water_kib * 1024;
    check_boundaries();
    const fixture frames(size);
    measure(frames, count / 10, high_water);
    std::printf("%s,%s,%zu,%.3f\n", argv[1], argv[2], count, measure(frames, count, high_water));
}
