#ifndef PORTAL_BENCHMARKS_RPC_RING_FF_H
#define PORTAL_BENCHMARKS_RPC_RING_FF_H

#include "rpc_ring.h"
#include "portal/rpc/typed_async.h"
#include "portal/extensions/flux_foundry/async.h"
#include <executor/simple_executor.h>
#include <thread>

namespace rpc_ring_bench {
namespace ff = flux_foundry;

struct space_request {
    byte_ring* ring;
    std::size_t bytes;
};

struct ring_space_awaitable final
    : ff::fast_awaitable_base<ring_space_awaitable, void, portal::async_error>, space_waiter {
    using async_result_type = ff::result_t<void, portal::async_error>;
    using input_type = ff::result_t<space_request, portal::async_error>;
    input_type input;

    explicit ring_space_awaitable(input_type&& request) noexcept : input(std::move(request)) {}
    bool available() const noexcept { return true; }

    int submit() noexcept {
        if (input.has_value()) {
            const auto request = input.value();
            request.ring->wait_async(request.bytes, *this);
        } else {
            this->resume(async_result_type(ff::error_tag, std::move(input.error())));
        }
        return 0;
    }

    void complete(std::error_code error) noexcept override {
        if (error) this->resume(async_result_type(ff::error_tag, portal::async_error(error)));
        else this->resume(async_result_type(ff::value_tag));
    }
};

using space_result = ring_space_awaitable::async_result_type;

inline portal::async_error typed_write_error(write_status status) noexcept {
    switch (status) {
    case write_status::published: return {};
    case write_status::oversized: return std::make_error_code(std::errc::message_size);
    case write_status::invalid: return portal::typed_rpc::make_error_code(portal::wire::errc::invalid_value);
    case write_status::full: return std::make_error_code(std::errc::no_buffer_space);
    }
    std::abort();
}

template<class Request, class Handler> struct retry_typed_write {
    struct pending_write {
        byte_ring& ring;
        Request request;
        Handler done;
    };
    using value_type = space_result;
    std::shared_ptr<pending_write> pending;
    void emplace(space_result&& result) noexcept {
        auto error = result.has_value() ? typed_write_error(pending->ring.try_write_typed(pending->request))
                                        : std::move(result.error());
        pending->done(std::move(error));
    }
};

// Single producer. The ring and the FF blueprint outlive the publication callback.
// The callback reports publication; the transport still owns response correlation.
template<class Blueprint, class Request, class Handler>
void async_write_typed(byte_ring& ring, Blueprint& blueprint, Request&& request, Handler done) {
    const auto status = ring.try_write_typed(request);
    if (status != write_status::full) {
        done(typed_write_error(status));
        return;
    }
    const auto size = request.size();
    using receiver = retry_typed_write<typename std::decay<Request>::type, Handler>;
    using pending = typename receiver::pending_write;
    auto work = std::make_shared<pending>(pending{ring, std::forward<Request>(request), std::move(done)});
    auto runner = ff::make_fast_runner_view(blueprint, receiver{std::move(work)});
    runner(space_request{&ring, size});
}

template<class Pump>
struct resume_producer {
    using value_type = space_result;
    Pump* pump;
    void emplace(value_type&& result) noexcept {
        check(result.has_value());
        (*pump)(*pump);
    }
};

// One active producer and at most one queued continuation. On pressure the
// pump returns to the executor, which may run other work while the ring drains.
template<class Submit>
void run_ff_producer(byte_ring& ring, std::size_t count, Submit& submit_frame) {
    ff::simple_executor<8> executor;
    auto blueprint = ff::make_blueprint<space_request, portal::async_error>()
        | ff::await<ring_space_awaitable>(&executor) | ff::end();
    std::size_t next = 0;
    const auto producer_thread = std::this_thread::get_id();
    auto pump = [&](auto& self) noexcept -> void {
        check(std::this_thread::get_id() == producer_thread);
        while (next < count) {
            const auto frame = submit_frame(next);
            if (frame.first == write_status::full) {
                auto runner = ff::make_fast_runner_view(blueprint,
                    resume_producer<typename std::decay<decltype(self)>::type>{&self});
                runner(space_request{&ring, frame.second});
                return;
            }
            check(frame.first == write_status::published);
            ++next;
        }
        executor.try_shutdown();
    };
    executor.dispatch(ff::task_wrapper_sbo([&]() noexcept { pump(pump); }));
    executor.run();
    check(next == count);
}

}
#endif
