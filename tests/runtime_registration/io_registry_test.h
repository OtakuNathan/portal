#pragma once
// Included by the integration and installed-package test.
namespace io_test {
using service_type = ext::io_registration_registry;
using result = service_type::status;
using task = flux_foundry::task_wrapper_sbo;

struct queue {
    enum mode { hold, full } behavior = hold;
    task pending;
    bool try_dispatch(task&& work) noexcept {
        if (behavior == full)
            return false;
        pending = std::move(work);
        return true;
    }
};
struct owner_inline_executor {
    void dispatch(task&& work) noexcept {
        work();
    }
};
struct receiver {
    using value_type = flux_foundry::result_t<result, std::exception_ptr>;
    std::promise<result>* completion;
    void emplace(value_type&& value) noexcept {
        if (value.has_value())
            completion->set_value(value.value());
        else
            completion->set_exception(value.error());
    }
};

void run(ext::module_source job) {
    std::atomic<unsigned> destroyed{0};
    {
        service_type service;
        queue io;
        auto bp = ext::registration_blueprint(service, io, &destroyed);
        auto submit = [&](std::promise<result>& done) {
            auto runner = flux_foundry::make_fast_runner_view(bp, receiver{&done});
            runner(ext::module_source(job));
        };
        std::promise<void> entered, release;
        auto resumed = release.get_future();
        auto old = service.prepare({}, [&](rpc::router& r) {
            return r.define<add<1>>([&](std::int32_t a, std::int32_t b) {
                entered.set_value();
                resumed.wait();
                return a + b;
            });
        });
        CHECK(std::move(old).install() == result::ok);
        w::bytes request, reply, latest;
        CHECK(rpc::encode_request<add<1>>(request, {}, std::int32_t(20), std::int32_t(22)) ==
              w::errc::ok);
        CHECK(rpc::encode_request<add<100>>(latest, {}, std::int32_t(20), std::int32_t(22)) ==
              w::errc::ok);
        auto held = service.resolve(w::view(request));
        auto worker = std::async(std::launch::async, [held, &reply] {
            return held.dispatch_into(reply);
        });
        entered.get_future().wait();

        // Both queue rejection and cancellation release the prepared module and allow a retry.
        io.behavior = queue::full;
        std::promise<result> declined;
        submit(declined);
        CHECK(declined.get_future().get() == result::not_delivered && destroyed == 1);
        io.behavior = queue::hold;
        std::promise<result> canceled;
        submit(canceled);
        CHECK(bool(io.pending));
        io.pending = task{};
        CHECK(canceled.get_future().get() == result::not_delivered && destroyed == 2);

        std::promise<result> delivered;
        auto ack = delivered.get_future();
        std::thread control([&] {
            submit(delivered);
        });
        control.join();
        CHECK(io.pending &&
              ack.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
        auto busy = service.prepare({}, [](rpc::router&) {
            return w::errc::ok;
        });
        CHECK(busy.result() == result::busy);
        w::bytes response;
        CHECK(service.dispatch(latest, response) == w::errc::unknown_method);
        io.pending();
        CHECK(ack.get() == result::ok);
        CHECK(service.dispatch(latest, response) == w::errc::ok);
        CHECK(rpc::decode_response<add<100>>(w::view(response)).value == 142);
        release.set_value();
        CHECK(worker.get() == w::errc::ok &&
              rpc::decode_response<add<1>>(w::view(reply)).value == 42);

        auto delayed = service.resolve(w::view(latest));
        auto pin = std::make_shared<int>(0);
        try {
            service.prepare(pin, [](rpc::router& r) -> w::errc {
                CHECK(r.define<add<2>>([](std::int32_t a, std::int32_t) {
                    return a;
                }) == w::errc::ok);
                throw std::logic_error("partial batch");
            });
            CHECK(false);
        } catch (const std::logic_error&) {
        }
        CHECK(pin.use_count() == 1);
        auto duplicate = service.prepare({}, [](rpc::router& r) {
            CHECK(r.define<add<2>>([](std::int32_t a, std::int32_t) {
                return a;
            }) == w::errc::ok);
            return r.define<add<100>>([](std::int32_t a, std::int32_t) {
                return a;
            });
        });
        CHECK(duplicate.result() == result::rejected);
        CHECK(rpc::encode_request<add<2>>(request, {}, std::int32_t(2), std::int32_t(3)) ==
              w::errc::ok);
        CHECK(service.dispatch(request, response) == w::errc::unknown_method);
        auto native = [](rpc::router& r) {
            return r.define<add<2>>([](std::int32_t a, std::int32_t b) {
                return a + b;
            });
        };
        auto wrong = service.prepare({}, native);
        CHECK(std::async(std::launch::async, [&] {
                  return std::move(wrong).install();
              }).get() == result::wrong_thread);
        auto retry = service.prepare({}, native);
        CHECK(std::move(retry).install() == result::ok);
        // Begin the saved call only after its lookup table has been reclaimed.
        CHECK(delayed.dispatch_into(response) == w::errc::ok);
        CHECK(rpc::decode_response<add<100>>(w::view(response)).value == 142);
        CHECK(service.dispatch(request, request) == w::errc::ok);
        CHECK(rpc::decode_response<add<2>>(w::view(request)).value == 5);

        // A fresh registry on this thread supports owner-inline delivery.
        service_type inline_service;
        owner_inline_executor immediate;
        auto inline_bp = ext::registration_blueprint(inline_service, immediate, &destroyed);
        std::promise<result> inline_done;
        auto runner = flux_foundry::make_fast_runner_view(inline_bp, receiver{&inline_done});
        runner(ext::module_source(job));
        CHECK(inline_done.get_future().get() == result::ok);
    }
    CHECK(destroyed == 4);
}
} // namespace io_test
