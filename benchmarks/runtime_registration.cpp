// Every build uses the same compiled handler, pre-encoded request and worker-local
// response buffer. A bare router measures the cost without concurrent registration.
#include "experiments/runtime_registration_original.h"
#include <portal/extensions/flux_foundry/registration_registry.h>
#include <portal/extensions/flux_foundry/runtime_module.h>
#include <portal/extensions/flux_foundry/io_registration_registry.h>
#include <executor/simple_executor.h>
#include <future>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

namespace ext = portal::ff;
namespace rpc = portal::typed_rpc;
namespace w = portal::wire;
using registry = ext::registration_registry;
using method = rpc::method<100, 1, w::i32, w::i32, w::i32>;
using timer = std::chrono::steady_clock;
static void check(bool ok) { if (!ok) std::abort(); }

template<class MakeDispatch>
double sample_worker(const w::bytes& request, unsigned workers, MakeDispatch make_dispatch) {
    constexpr int iterations = 100000;
    std::atomic<unsigned> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < workers; ++i) threads.emplace_back([&] {
        auto dispatch = make_dispatch();
        w::bytes reply;
        auto invoke = [&] {
            check(dispatch(request, reply) == w::errc::ok);
            const auto result = rpc::decode_response<method>(w::view(reply));
            check(result && result.value == 142);
        };
        for (int j = 0; j < 100; ++j) invoke();
        ++ready;
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        for (int j = 0; j < iterations; ++j) invoke();
    });
    while (ready != workers) std::this_thread::yield();
    const auto begin = timer::now();
    start.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();
    return std::chrono::duration<double, std::nano>(timer::now() - begin).count() / (iterations * workers);
}

using io_registry = ext::io_registration_registry;

// Time a real cross-thread submission and owner acknowledgment separately from compilation.
void sample_install(const std::shared_ptr<ext::runtime_module>& module, int rep) {
    flux_foundry::simple_executor<16> executor;
    std::promise<io_registry*> ready;
    std::thread io([&] {
        io_registry service;
        ready.set_value(&service);
        executor.run();
    });
    auto* service = ready.get_future().get();
    std::promise<void> installed;
    auto ack = installed.get_future();
    const auto begin = timer::now();
    auto update = service->prepare(module, [&](rpc::router& r) { return module->register_into(r, nullptr); });
    check(update.result() == io_registry::status::ok);
    const auto prepared = timer::now();
    executor.dispatch(flux_foundry::task_wrapper_sbo([&]() noexcept {
        check(std::move(update).install() == io_registry::status::ok);
        installed.set_value();
    }));
    ack.get();
    const auto end = timer::now();
    executor.try_shutdown();
    io.join();
    std::printf("cow_prepare,0,%d,%.0f\ncow_submit_ack,0,%d,%.0f\n", rep,
        std::chrono::duration<double, std::nano>(prepared-begin).count(), rep,
        std::chrono::duration<double, std::nano>(end-prepared).count());
}

int main(int argc, char** argv) {
    if (argc != 4) {
        std::fputs("usage: runtime_registration_benchmark CXX PORTAL_INCLUDE tests/runtime_registration/module.cpp\n", stderr);
        return 2;
    }
    ext::module_source source{argv[3], argv[1], {std::string("-I") + argv[2]}};
    w::bytes request;
    check(rpc::encode_request<method>(request, {}, std::int32_t(20), std::int32_t(22)) == w::errc::ok);
    std::puts("phase,workers,repeat,ns_per_operation");
    for (int rep = 0; rep < 5; ++rep) {
        registry service;
        const auto begin = timer::now();
        auto module = ext::runtime_module::compile(source);
        const auto loaded = timer::now();
        const auto status = service.publish(module, [&](rpc::router& r) { return module->register_into(r, nullptr); });
        const auto published = timer::now();
        sample_install(module, rep);
        check(status == registry::update_result::ok);
        w::bytes reply;
        check(service.dispatch(request, reply) == w::errc::ok);
        const auto decoded = rpc::decode_response<method>(w::view(reply));
        check(decoded && decoded.value == 142);
        std::printf("compile_load,0,%d,%.0f\npublish,0,%d,%.0f\n", rep,
            std::chrono::duration<double, std::nano>(loaded-begin).count(), rep,
            std::chrono::duration<double, std::nano>(published-loaded).count());
    }
    auto module = ext::runtime_module::compile(source);
    rpc::router bare;
    registry service;
    check(module->register_into(bare, nullptr) == w::errc::ok);
    check(service.publish(module, [&](rpc::router& r) { return module->register_into(r, nullptr); })
          == registry::update_result::ok);
    registration_prototype::coordinator prototype;
    check(prototype.publish(module, [&](rpc::router& r) { return module->register_into(r, nullptr); })
          == registration_prototype::coordinator::update_result::ok);
    for (unsigned workers : {1u, 4u}) {
        for (int rep = 0; rep < 7; ++rep) {
            for (int turn = 0; turn < 4; ++turn) {
                const int lane = (rep + turn) % 4;
                const auto measure = [&](const char* name, auto dispatch) {
                    const auto ns = sample_worker(request, workers, [&] { return dispatch; });
                    std::printf("%s,%u,%d,%.2f\n", name, workers, rep, ns);
                };
                if (lane == 0) measure("bare", [&](const auto& in, auto& out) { return bare.dispatch_into(in, out); });
                if (lane == 1) measure("prototype", [&](const auto& in, auto& out) { return prototype.dispatch(in, out); });
                if (lane == 2) measure("registry", [&](const auto& in, auto& out) { return service.dispatch(in, out); });
                if (lane == 3) {
                    const auto ns = sample_worker(request, workers, [&] {
                        auto io = std::make_unique<io_registry>();
                        auto update = io->prepare(module, [&](rpc::router& r) { return module->register_into(r, nullptr); });
                        check(std::move(update).install() == io_registry::status::ok);
                        return [io = std::move(io)](const auto& in, auto& out) { return io->dispatch(in, out); };
                    });
                    std::printf("cow_io,%u,%d,%.2f\n", workers, rep, ns);
                }
            }
        }
    }
}
