#include <portal/extensions/flux_foundry/runtime_registration.h>
#include <atomic>
#include <cstdio>
#include <future>
#include <thread>

namespace rpc = portal::typed_rpc;
namespace w = portal::wire;
namespace ext = portal::ff;
using registry = ext::registration_registry;
using status = registry::update_result;
template <std::uint64_t Id>
using add = rpc::method<Id, 1, w::i32, w::i32, w::i32>;

#define CHECK(expr)                                                                                \
    do {                                                                                           \
        if (!(expr)) {                                                                             \
            std::fprintf(stderr, "failed at %d: %s\n", __LINE__, #expr);                           \
            std::abort();                                                                          \
        }                                                                                          \
    } while (false)

template <class Method>
auto call(registry& service, std::int32_t a = 20, std::int32_t b = 22) {
    w::bytes request, reply;
    CHECK(rpc::encode_request<Method>(request, {}, a, b) == w::errc::ok);
    service.dispatch(request, reply);
    return rpc::decode_response<Method>(w::view(reply));
}

struct outcome {
    using value_type = flux_foundry::result_t<status, std::exception_ptr>;
    status* registered;
    std::exception_ptr* failure;
    void emplace(value_type&& result) noexcept {
        if (result.has_value())
            *registered = result.value();
        else
            *failure = std::move(result).error();
    }
};

template <class Blueprint>
status run(Blueprint& bp, ext::module_source job) {
    status registered = status::rejected;
    std::exception_ptr failure;
    auto runner = flux_foundry::make_fast_runner_view(bp, outcome{&registered, &failure});
    runner(std::move(job));
    if (failure)
        std::rethrow_exception(failure);
    return registered;
}

template <class Predicate>
void wait_until(Predicate ready) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!ready()) {
        CHECK(std::chrono::steady_clock::now() < end);
        std::this_thread::yield();
    }
}

template <unsigned Id>
void add_native_method(registry& service) {
    CHECK(service.publish({}, [](rpc::router& r) {
        return r.define<add<Id>>([](std::int32_t a, std::int32_t) {
            return a;
        });
    }) == status::ok);
}

void check_overlapping_readers() {
    registry service;
    std::atomic<unsigned> entered{0};
    std::promise<void> release_group, release_last;
    auto group = release_group.get_future().share();
    auto last = release_last.get_future().share();
    CHECK(service.publish({}, [&](rpc::router& r) {
        return r.define<add<1>>([&](std::int32_t i, std::int32_t) {
            ++entered;
            (i ? group : last).wait();
            return i;
        });
    }) == status::ok);
    std::vector<std::future<void>> readers;
    for (int i = 0; i < 33; ++i)
        readers.push_back(std::async(std::launch::async, [&, i] {
            auto result = call<add<1>>(service, i);
            CHECK(result && result.value == i);
        }));
    wait_until([&] {
        return entered.load() == readers.size();
    });
    CHECK(service.state().active == readers.size());
    auto writer = std::async(std::launch::async, [&] {
        return service.publish(
            {},
            [](rpc::router&) {
                return w::errc::ok;
            },
            std::chrono::seconds(30));
    });
    wait_until([&] {
        return service.state().updating;
    });
    release_group.set_value();
    for (std::size_t i = 1; i < readers.size(); ++i)
        readers[i].get();
    const auto state = service.state();
    CHECK(state.updating && state.active == 1);
    release_last.set_value();
    readers.front().get();
    CHECK(writer.get() == status::ok);
}

#include "io_registry_test.h"

int main(int argc, char** argv) {
    CHECK(argc >= 4);
    check_overlapping_readers();
    ext::module_source job{argv[3], argv[1], {std::string("-I") + argv[2]}};
    for (int i = 4; i < argc; ++i)
        job.compiler_flags.emplace_back(argv[i]);
    std::string temp = job.temporary_directory + "/portal-registration-test-XXXXXX";
    CHECK(::mkdtemp(temp.data()));
    job.temporary_directory = temp;
    io_test::run(job);
    std::atomic<unsigned> destroyed{0};
    bool released_after_handlers = false;
    {
        registry service;
        std::promise<void> entered, release;
        auto released = release.get_future().share();
        CHECK(service.publish({}, [&](rpc::router& r) {
            return r.define<add<1>>([&](std::int32_t a, std::int32_t b) {
                entered.set_value();
                released.wait();
                CHECK(service.publish({}, [](rpc::router&) {
                    return w::errc::ok;
                }) == status::reentrant);
                CHECK(call<add<100>>(service).error == w::errc::handler_failed);
                return a + b;
            });
        }) == status::ok);
        CHECK(call<add<100>>(service).error == w::errc::unknown_method);
        auto old = std::async(std::launch::async, [&] {
            return call<add<1>>(service);
        });
        entered.get_future().wait();
        CHECK(service.publish(
                  {},
                  [](rpc::router&) {
                      return w::errc::ok;
                  },
                  std::chrono::milliseconds(5)) == status::timeout);
        CHECK(!service.state().updating && service.state().active == 1);
        // Compilation fails outside the gate even with an outstanding read loan.
        auto bp = ext::registration_blueprint(service, &destroyed, std::chrono::seconds(10));
        auto bad = job;
        bad.compiler_flags.push_back("-include");
        bad.compiler_flags.push_back("portal_intentionally_missing_registration_header.h");
        bool failed = false;
        try {
            run(bp, bad);
        } catch (const std::runtime_error&) {
            failed = true;
        }
        CHECK(failed && !service.state().updating);

        bad = job;
        bad.compiler_flags.push_back("-Dportal_register_rpc_v1=unused_entry");
        failed = false;
        try {
            run(bp, bad);
        } catch (const std::runtime_error&) {
            failed = true;
        }
        CHECK(failed && !service.state().updating);
        auto publisher = std::async(std::launch::async, [&] {
            return run(bp, job);
        });
        wait_until([&] {
            return service.state().updating;
        });
        CHECK(service.publish({}, [](rpc::router&) {
            return w::errc::ok;
        }) == status::busy);
        std::vector<std::future<int>> waiting;
        for (int i = 0; i < 16; ++i)
            waiting.push_back(std::async(std::launch::async, [&] {
                auto answer = call<add<100>>(service);
                CHECK(answer);
                return answer.value;
            }));
        wait_until([&] {
            return service.state().waiting == waiting.size();
        });
        release.set_value();
        CHECK(old.get().value == 42 && publisher.get() == status::ok);
        for (auto& answer : waiting)
            CHECK(answer.get() == 142);
        CHECK(call<add<100>>(service, INT32_MAX, INT32_MAX).value == INT32_MAX);
        CHECK(call<add<100>>(service, INT32_MIN, INT32_MIN).value == INT32_MIN);
        CHECK(run(bp, job) == status::rejected && destroyed == 1);

        // A module pin survives a partial append followed by an exception.
        auto pin = std::shared_ptr<int>(new int(23), [&](int* value) noexcept {
            released_after_handlers = destroyed == 2;
            delete value;
        });
        std::weak_ptr<int> observer = pin;
        try {
            service.publish(pin, [&](rpc::router& r) -> w::errc {
                CHECK(service.publish({}, [](rpc::router&) {
                    return w::errc::ok;
                }) == status::reentrant);
                CHECK(call<add<100>>(service).error == w::errc::handler_failed);
                CHECK(r.define<add<2>>([observer](std::int32_t, std::int32_t) {
                    return std::int32_t(*observer.lock());
                }) == w::errc::ok);
                throw std::logic_error("injected registration failure");
            });
            CHECK(false);
        } catch (const std::logic_error&) {
        }
        pin.reset();
        CHECK(call<add<2>>(service).value == 23 && !observer.expired());
        std::vector<std::thread> readers;
        for (int t = 0; t < 4; ++t)
            readers.emplace_back([&] {
                for (int i = 0; i < 10000; ++i)
                    CHECK(call<add<100>>(service).value == 142);
            });
        add_native_method<201>(service);
        add_native_method<202>(service);
        add_native_method<203>(service);
        add_native_method<204>(service);
        add_native_method<205>(service);
        add_native_method<206>(service);
        add_native_method<207>(service);
        add_native_method<208>(service);
        for (int i = 0; i < 200; ++i) {
            auto duplicate = std::make_shared<int>(1);
            CHECK(service.publish(duplicate, [](rpc::router& r) {
                return r.define<add<2>>([](std::int32_t, std::int32_t) {
                    return std::int32_t(0);
                });
            }) == status::rejected);
            CHECK(duplicate.use_count() == 1);
        }
        for (auto& reader : readers)
            reader.join();
        CHECK(destroyed == 1);
    }
    CHECK(destroyed == 2 && released_after_handlers);
    CHECK(!::rmdir(temp.c_str()));
}
