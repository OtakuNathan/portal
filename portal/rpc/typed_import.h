#ifndef PORTAL_RPC_TYPED_IMPORT_H
#define PORTAL_RPC_TYPED_IMPORT_H
#include "../backend_base.h"
#include "typed_async.h"

namespace portal { namespace typed_rpc {
namespace detail {
template<class Signature> struct import_signature;
template<class R, class... A> struct import_signature<R(A...)> {
    using type = R(typename std::decay<A>::type...);
    using result = R;
};
}

// Routes<Symbol, Signature>::type names a typed method. Each service view
// addresses the endpoint carried by its context. There is no remote object ID.
template<class Transport, template<class, class> class Routes>
struct direct_import_backend : backend_base<direct_import_backend<Transport, Routes>> {
    template<class, class> struct object_t {};

    struct context {
        using backend_t = direct_import_backend;
        std::shared_ptr<Transport> transport;
        limits budget;
        explicit context(std::shared_ptr<Transport> io, limits l = {})
            : transport(std::move(io)), budget(l) { async_detail::require(bool(transport)); }
    };

    template<class Symbol, class Contract, class Receiver, class... A>
    static auto prepare_async(std::shared_ptr<Receiver> receiver, std::tuple<A...>&& args) {
        return prepare<Symbol, Contract>(receiver->context(), std::move(args), std::index_sequence_for<A...>{});
    }

    template<class Symbol, class Contract, class... A>
    static auto prepare_async_free(std::shared_ptr<context> owner, std::tuple<A...>&& args) {
        return prepare<Symbol, Contract>(*owner, std::move(args), std::index_sequence_for<A...>{});
    }

private:
    template<class Symbol, class Contract, class Tuple, std::size_t... I>
    static auto prepare(const context& endpoint, Tuple&& args, std::index_sequence<I...>) {
        using signature = typename Contract::signature_t;
        using method = typename Routes<Symbol, signature>::type;
        using declared = detail::import_signature<signature>;
        using R = typename declared::result;
        static_assert(std::is_same<typename declared::type, typename method::signature_t>::value,
                      "typed RPC method must match the selected import signature");
        return detail::make_direct_call<method, R>(endpoint.transport, endpoint.budget,
                                                 std::get<I>(std::forward<Tuple>(args))...);
    }
};
}}
#endif
