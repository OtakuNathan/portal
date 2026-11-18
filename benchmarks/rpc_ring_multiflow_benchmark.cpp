// Four independent RPC streams, one producer executor and one I/O consumer.
// Both producers yield their turn after the same number of published frames.
#include "rpc_ring_ff.h"

#include <array>
#include <chrono>
#include <limits>
#include <string>

namespace {
using namespace rpc_ring_bench;
using clock_type = std::chrono::steady_clock;
constexpr std::size_t stream_count = 4;
using executor = ff::simple_executor<8>;
enum class scenario { balanced, paused, gated_test };

struct ring_sizes {
    std::size_t capacity;
    std::size_t high_water;
};

struct stream {
    byte_ring ring;
    value payload;
    bytes expected;
    alignas(64) std::size_t sent = 0;
    alignas(64) std::size_t received = 0;
    std::size_t offset = 0;

    stream(ring_sizes sizes) : ring(sizes.capacity, sizes.high_water) {}

    void init(std::size_t id) {
        std::string body(1024, '\0');
        for (std::size_t i = 0; i < body.size(); ++i)
            body[i] = static_cast<char>((i + id) % 251);
        payload = value::string(std::move(body));
        expected = codec::encode_request_values(0, payload);
    }

    template<bool Async>
    write_status publish() {
        auto encode = [&](auto& out) {
            out.request_header(sent, 1);
            codec::encode_value(out, payload);
        };
        return Async ? ring.try_write(expected.size(), encode) : ring.write(expected.size(), encode);
    }

    std::size_t read() {
        return ring.read([&](const std::uint8_t* data, std::size_t size) {
            const auto accepted = std::min<std::size_t>(size, 4096);
            std::size_t checked = 0;
            while (checked < accepted) {
                const auto part = std::min(accepted - checked, expected.size() - offset);
                check(std::memcmp(data + checked, expected.data() + offset, part) == 0);
                checked += part;
                offset += part;
                if (offset == expected.size()) {
                    ++received;
                    offset = 0;
                    // Each frame carries its unique sequence in the LE method field.
                    const std::uint64_t sequence = received;
                    for (unsigned i = 0; i < 8; ++i)
                        expected[5 + i] = static_cast<std::uint8_t>(sequence >> (8 * i));
                }
            }
            return accepted;
        });
    }
};
using streams = std::array<stream, stream_count>;

auto make_wait_pipeline(executor& exec) {
    return ff::make_blueprint<space_request, portal::async_error>()
        | ff::await<ring_space_awaitable>(&exec) | ff::end();
}

template<bool Async>
class producers {
    streams& flows_;
    const std::size_t count_, quantum_;
    executor exec_;
    decltype(make_wait_pipeline(exec_)) blueprint_;
    std::size_t active_ = stream_count;
    struct resumed {
        using value_type = space_result;
        producers* self;
        std::size_t id;
        void emplace(space_result&& result) noexcept {
            check(result.has_value());
            self->turn(id);
        }
    };

    void schedule(std::size_t id) {
        exec_.dispatch(ff::task_wrapper_sbo([this, id]() noexcept { turn(id); }));
    }
    void turn(std::size_t id) {
        auto& flow = flows_[id];
        const auto end = std::min(count_, flow.sent + quantum_);
        while (flow.sent < end) {
            const auto status = flow.publish<Async>();
            if (status == write_status::full) {
                check(Async);
                auto runner = ff::make_fast_runner_view(blueprint_, resumed{this, id});
                runner(space_request{&flow.ring, flow.expected.size()});
                return;
            }
            check(status == write_status::published);
            ++flow.sent;
        }
        if (flow.sent < count_) schedule(id);
        else if (--active_ == 0) exec_.try_shutdown();
    }
public:
    producers(streams& flows, std::size_t count, std::size_t quantum)
        : flows_(flows), count_(count), quantum_(quantum), blueprint_(make_wait_pipeline(exec_)) {}
    void run() {
        // One runnable task or pending wait per stream. The queue holds all four.
        for (std::size_t i = 0; i < stream_count; ++i) schedule(i);
        exec_.run();
    }
};

struct metrics {
    double total_ms = 0, healthy_done_ms = 0;
    std::size_t healthy_during_pause = 0;
};

double elapsed_ms(clock_type::time_point start) {
    return std::chrono::duration<double, std::milli>(clock_type::now() - start).count();
}

template<bool Async>
metrics measure(scenario mode, std::size_t quantum, std::size_t count, ring_sizes sizes) {
    streams flows{{{sizes}, {sizes}, {sizes}, {sizes}}};
    for (std::size_t i = 0; i < stream_count; ++i) flows[i].init(i);
    producers<Async> producer(flows, count, quantum);
    std::atomic<bool> ready{false}, go{false};
    clock_type::time_point start;
    metrics result;
    std::thread io([&] {
        std::size_t healthy = 0;
        bool paused = mode == scenario::gated_test, pause_started = paused;
        clock_type::time_point reopen;
        ready.store(true, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
        while (flows[0].received < count || healthy < (stream_count - 1) * count) {
            if (mode == scenario::paused && !pause_started && flows[0].received >= count / 8) {
                paused = pause_started = true;
                reopen = clock_type::now() + std::chrono::milliseconds(40);
            }
            if (paused && mode == scenario::paused && clock_type::now() >= reopen) paused = false;
            if (paused && mode == scenario::gated_test && healthy == (stream_count - 1) * count
                    && flows[0].ring.producer_paused()) paused = false;

            std::size_t bytes_read = 0;
            for (std::size_t id = paused ? 1 : 0; id < stream_count; ++id) {
                auto& flow = flows[id];
                if (flow.received == count) continue;
                const auto before = flow.received;
                bytes_read += flow.read();
                check(flow.received <= count);
                if (id != 0) {
                    const auto frames = flow.received - before;
                    healthy += frames;
                    if (paused) result.healthy_during_pause += frames;
                    if (frames && healthy == (stream_count - 1) * count)
                        result.healthy_done_ms = elapsed_ms(start);
                }
            }
            if (!bytes_read) std::this_thread::yield();
        }
        result.total_ms = elapsed_ms(start);
    });
    while (!ready.load(std::memory_order_acquire)) std::this_thread::yield();
    start = clock_type::now();
    go.store(true, std::memory_order_release);
    producer.run();
    io.join();
    for (const auto& flow : flows)
        check(flow.sent == count && flow.received == count && flow.offset == 0);
    return result;
}
}

int main(int argc, char** argv) {
    ring_sizes sizes{1024 * 1024, 256 * 1024};
    if (argc == 2 && std::strcmp(argv[1], "--check") == 0) {
        constexpr std::size_t count = 1024;
        const auto result = measure<true>(scenario::gated_test, 16, count, sizes);
        check(result.healthy_during_pause == (stream_count - 1) * count);
        measure<false>(scenario::balanced, 16, count, sizes);
        return 0;
    }
    if (argc != 4 && argc != 6) {
        std::fputs("usage: rpc_ring_multiflow_benchmark cv|ff balanced|paused frames-per-turn [ring-KiB high-water-KiB]\n", stderr);
        return 2;
    }
    const std::string lane = argv[1], profile = argv[2];
    if ((lane != "cv" && lane != "ff") || (profile != "balanced" && profile != "paused")) return 2;
    char* end = nullptr;
    const auto quantum = std::strtoul(argv[3], &end, 10);
    if (*end || !quantum || quantum > 1024) return 2;
    if (argc == 6) {
        const auto capacity = std::strtoul(argv[4], &end, 10);
        if (argv[4][0] == '-' || *end || capacity < 2 || (capacity & (capacity - 1))
                || capacity > (std::numeric_limits<std::size_t>::max)() / 1024) return 2;
        const auto high = std::strtoul(argv[5], &end, 10);
        if (*end || !high || high > capacity) return 2;
        sizes = {capacity * 1024, high * 1024};
    }
    const auto run = lane == "ff" ? &measure<true> : &measure<false>;
    constexpr std::size_t frames_per_stream = 16000;
    run(scenario::balanced, quantum, 1000, sizes);
    const auto result = run(profile == "paused" ? scenario::paused : scenario::balanced, quantum, frames_per_stream, sizes);
    std::printf("%s,%s,%lu,%zu,%.3f,%.3f,%zu\n", argv[1], argv[2], quantum,
        frames_per_stream * stream_count, result.total_ms, result.healthy_done_ms, result.healthy_during_pause);
}
