#ifndef PORTAL_RPC_TYPED_EXPORT_H
#define PORTAL_RPC_TYPED_EXPORT_H

#include "../export_core.h"
#include "typed.h"

namespace portal {
namespace typed_rpc {
namespace detail {
template <class Signature>
struct export_signature;

template <class R, class... Args>
struct export_signature<R(Args...)> {
    static_assert(
        !std::is_reference<R>::value,
        "typed RPC export return type cannot be a reference; use a value or void instead");
    static_assert(are_bridge_params_valid<Args...>::value,
                  "typed RPC export arguments cannot be non-const lvalue references");
    using type = R(typename std::decay<Args>::type...);
};

template <class Fn, class Signature>
struct free_export_invoker;

template <class Fn, class R, class... Args>
struct free_export_invoker<Fn, R(Args...)> {
    Fn function;

    decltype(auto) operator()(Args&&... args) const {
        return function(std::forward<Args>(args)...);
    }
};

template <class ClassProxy, class MemberTag, class Signature>
struct member_export_invoker;

template <class ClassProxy, class MemberTag, class R, class... Args>
struct member_export_invoker<ClassProxy, MemberTag, R(Args...)> {
    typename ClassProxy::native_t* target;

    decltype(auto) operator()(Args&&... args) const {
        return MemberTag::call_native(*target, std::forward<Args>(args)...);
    }
};

template <class ClassProxy, template <class, class> class Routes>
class export_class_visitor {
    router& server_;
    typename ClassProxy::native_t& target_;
    wire::errc status_ = wire::errc::ok;

public:
    export_class_visitor(router& server, typename ClassProxy::native_t& target) noexcept
        : server_(server), target_(target) {}

    wire::errc status() const noexcept {
        return status_;
    }

    // Typed RPC services address a bound endpoint/instance rather than remote object IDs.
    template <class... Args>
    void constructor() noexcept {}

    template <class Signature, class MemberTag>
    void member(const char*, export_member_forwarder<MemberTag, Signature>) {
        if (status_ != wire::errc::ok) {
            return;
        }
        using method_t = typename Routes<MemberTag, Signature>::type;
        using normalized_t = typename export_signature<Signature>::type;
        static_assert(std::is_same<normalized_t, typename method_t::signature_t>::value,
                      "typed RPC method must match the selected export signature");
        status_ = server_.template define<method_t>(
            member_export_invoker<ClassProxy, MemberTag, Signature>{&target_});
    }
};
} // namespace detail

// Register one declared free-function overload from PORTAL_EXPORT_DEF on a typed_rpc::router.
// Routes<ExportGroup, Signature>::type supplies the wire method descriptor.
template <class ExportGroup, class Signature, template <class, class> class Routes, class Fn>
wire::errc define_export(router& server, Fn&& fn) {
    static_assert(is_declared_free_callable<Signature, typename ExportGroup::group_t>::value,
                  "typed RPC export signature is not declared in this export group");
    using method_t = typename Routes<ExportGroup, Signature>::type;
    using normalized_t = typename detail::export_signature<Signature>::type;
    static_assert(std::is_same<normalized_t, typename method_t::signature_t>::value,
                  "typed RPC method must match the selected export signature");
    using invoker_t = detail::free_export_invoker<typename std::decay<Fn>::type, Signature>;
    return server.template define<method_t>(invoker_t{std::forward<Fn>(fn)});
}

template <class ExportGroup, template <class, class> class Routes, class R, class... Args>
wire::errc define_export(router& server, R (*fn)(Args...)) {
    return define_export<ExportGroup, R(Args...), Routes>(server, fn);
}

// Register all declared member overloads (including flattened IMPLEMENTS interfaces)
// of a PORTAL_EXPORT_DEF class onto a typed_rpc::router, borrowing the native target.
// Caller must keep the native instance alive while the router dispatches requests.
template <class ClassProxy, template <class, class> class Routes>
wire::errc export_class(router& server, typename ClassProxy::native_t& native) {
    router pending;
    detail::export_class_visitor<ClassProxy, Routes> visitor(pending, native);
    export_class_registrar<ClassProxy>::register_all(visitor);
    return visitor.status() == wire::errc::ok ? server.merge(std::move(pending)) : visitor.status();
}

template <class ClassProxy, template <class, class> class Routes>
wire::errc export_class(router& server, ClassProxy& proxy) {
    return export_class<ClassProxy, Routes>(server, proxy.native());
}
} // namespace typed_rpc
} // namespace portal

#endif // PORTAL_RPC_TYPED_EXPORT_H
