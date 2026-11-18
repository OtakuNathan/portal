#ifndef PORTAL_FF_RUNTIME_REGISTRATION_H
#define PORTAL_FF_RUNTIME_REGISTRATION_H

#include "registration_registry.h"
#include "io_registration_registry.h"
#include "runtime_module.h"
#include <flow/flow.h>

namespace portal { namespace ff {
namespace detail {
inline auto compile_module_blueprint() {
    namespace flow = flux_foundry;
    using input = flow::result_t<module_source, std::exception_ptr>;
    using compiled = flow::result_t<std::shared_ptr<runtime_module>, std::exception_ptr>;
    return flow::make_blueprint<module_source, std::exception_ptr>()
        | flow::then([](input&& job) noexcept -> compiled {
            if (!job.has_value()) return compiled(flow::error_tag, std::move(job).error());
            try { return compiled(flow::value_tag, runtime_module::compile(job.value())); }
            catch (...) { return compiled(flow::error_tag, std::current_exception()); }
        });
}
}
// The control thread running this blueprint performs both synchronous stages.
inline auto registration_blueprint(registration_registry& registry, void* context = nullptr,
        std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
    namespace flow = flux_foundry;
    using compiled = flow::result_t<std::shared_ptr<runtime_module>, std::exception_ptr>;
    using result = flow::result_t<registration_registry::update_result, std::exception_ptr>;
    return detail::compile_module_blueprint()
        | flow::then([&registry, context, timeout](compiled&& loaded) noexcept -> result {
            if (!loaded.has_value()) return result(flow::error_tag, std::move(loaded).error());
            try {
                auto module = std::move(loaded).value();
                auto status = registry.publish(module, [&](typed_rpc::router& r) {
                    return module->register_into(r, context);
                }, timeout);
                return result(flow::value_tag, status);
            } catch (...) { return result(flow::error_tag, std::current_exception()); }
        }) | flow::end();
}
namespace detail {
// Prefer the rejection-aware API on bounded executors. Ordinary FF executors
// guarantee delivery through dispatch() while accepting submissions.
template<class E>
auto post_install(E& io, flux_foundry::task_wrapper_sbo& task, int) noexcept
    -> decltype(void(io.try_dispatch(std::move(task)))) {
    static_assert(noexcept(io.try_dispatch(std::move(task))), "I/O submission must be noexcept");
    (void)io.try_dispatch(std::move(task));
}
template<class E>
void post_install(E& io, flux_foundry::task_wrapper_sbo& task, long) noexcept {
    static_assert(noexcept(io.dispatch(std::move(task))), "I/O submission must be noexcept");
    io.dispatch(std::move(task));
}
template<class Executor> struct install_request {
    Executor* executor;
    io_registration_registry::update update;
};
template<class Executor> class install_awaitable final
    : public flux_foundry::fast_awaitable_base<install_awaitable<Executor>,
          io_registration_registry::status, std::exception_ptr> {
    using status = io_registration_registry::status;
    using input_type = flux_foundry::result_t<install_request<Executor>, std::exception_ptr>;
    input_type input_;

    // The queue owns the one completion. Dropping a rejected or canceled task
    // destroys the staged batch and releases writer admission before resuming FF.
    struct delivery {
        install_awaitable* target;
        explicit delivery(install_awaitable* t) noexcept : target(t) {}
        delivery(delivery&& rhs) noexcept : target(std::exchange(rhs.target, nullptr)) {}
        ~delivery() { if (target) target->finish(false); }
        void operator()() noexcept {
            if (auto* t = std::exchange(target, nullptr)) t->finish(true);
        }
    };
    void finish(bool delivered) noexcept {
        status result = status::not_delivered;
        {
            auto update = std::move(input_.value().update);
            if (delivered) result = std::move(update).install();
        }
        this->resume(async_result_type(flux_foundry::value_tag, result));
    }
public:
    using async_result_type = flux_foundry::result_t<status, std::exception_ptr>;
    explicit install_awaitable(input_type&& input) noexcept : input_(std::move(input)) {}
    bool available() const noexcept { return true; }
    int submit() noexcept {
        if (!input_.has_value()) {
            this->resume(async_result_type(flux_foundry::error_tag, std::move(input_.error())));
        } else if (input_.value().update.result() != status::ok) {
            this->resume(async_result_type(flux_foundry::value_tag, input_.value().update.result()));
        } else {
            auto* executor = input_.value().executor;
            flux_foundry::task_wrapper_sbo task{delivery(this)};
            post_install(*executor, task, 0);
        }
        return 0;
    }
};
}

template<class Executor>
auto registration_blueprint(io_registration_registry& registry, Executor& io, void* context = nullptr) {
    namespace flow = flux_foundry;
    using compiled = flow::result_t<std::shared_ptr<runtime_module>, std::exception_ptr>;
    using request = detail::install_request<Executor>;
    using prepared = flow::result_t<request, std::exception_ptr>;
    return detail::compile_module_blueprint()
        | flow::then([&registry, &io, context](compiled&& loaded) noexcept -> prepared {
            if (!loaded.has_value()) return prepared(flow::error_tag, std::move(loaded).error());
            try {
                auto module = std::move(loaded).value();
                auto update = registry.prepare(module, [&](typed_rpc::router& r) {
                    return module->register_into(r, context);
                });
                return prepared(flow::value_tag, request{&io, std::move(update)});
            } catch (...) { return prepared(flow::error_tag, std::current_exception()); }
        }) | flow::await<detail::install_awaitable<Executor>>() | flow::end();
}
}}
#endif
