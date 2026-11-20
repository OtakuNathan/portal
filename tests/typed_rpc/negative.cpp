#include "portal/rpc/typed.h"
namespace w = portal::wire;
namespace tr = portal::typed_rpc;
struct Bad {
    std::uint32_t a = 0, b = 0;
};
#if NEGATIVE_CASE == 7
#include "portal/rpc/typed_import.h"
#define PORTAL_IMPORT_DEF "tests/typed_rpc/import.def"
#include "portal/import.h"
#undef PORTAL_IMPORT_DEF
template <class, class>
struct wrong_route {
    using type = tr::method<1, 1, w::i32>;
};
#elif NEGATIVE_CASE == 8 || NEGATIVE_CASE == 9 || NEGATIVE_CASE == 11 || NEGATIVE_CASE == 12
#include "portal/rpc/typed_export.h"
struct Packet {};
#define PORTAL_EXPORT_DEF "tests/typed_rpc/export.def"
#include "portal/export.h"
#undef PORTAL_EXPORT_DEF
template <class, class>
struct wrong_export_route {
    using type = tr::method<1, 1, w::i32>;
};
#if NEGATIVE_CASE == 11 || NEGATIVE_CASE == 12
template <class Symbol, class Signature>
struct matching_route {
    using type = tr::signature_method<1, 1, Signature>;
};
#endif
#elif NEGATIVE_CASE == 0
#define F(X, T) X(T, 1, a, w::u32) X(T, 2, b, w::u32)
PORTAL_WIRE_RECORD(Bad, 1, 1, F);
#elif NEGATIVE_CASE == 1
#define F(X, T) X(T, 1, a, w::u32) X(T, 1, b, w::u32)
PORTAL_WIRE_RECORD(Bad, 1, 1, F);
#elif NEGATIVE_CASE == 2
#define F(X, T) X(T, 1, a, w::u64)
PORTAL_WIRE_RECORD(Bad, 1, 1, F);
#endif
int main() {
#if NEGATIVE_CASE == 0
    w::bytes out;
    Bad b;
    if (w::encode_into<w::record<Bad>>(b, out) != w::errc::ok)
        return 1;
    auto decoded = w::decode<w::record<Bad>>(w::view(out));
    using M = tr::method<1, 1, w::i32, w::i32>;
    tr::router r;
    if (r.define<M>([](std::int32_t v) {
            return v;
        }) != w::errc::ok)
        return 2;
    if (tr::encode_request<M>(out, {}, std::int32_t(7)) != w::errc::ok)
        return 3;
    return decoded ? 0 : 4;
#elif NEGATIVE_CASE == 3
    w::bytes out;
    Bad b;
    w::encode_into<w::record<Bad>>(b, out);
#elif NEGATIVE_CASE == 4
    using M = tr::method<1, 1, w::i32, w::i32>;
    w::bytes out;
    tr::encode_request<M>(out, {}, std::int64_t(7));
#elif NEGATIVE_CASE == 5
    using M = tr::method<1, 1, w::i32, w::i32>;
    tr::router r;
    r.define<M>([](std::int32_t v) {
        return std::int64_t(v);
    });
#elif NEGATIVE_CASE == 6
    w::sequence<w::boolean, 4>::value_type x;
    (void)x;
#elif NEGATIVE_CASE == 7
    using context = tr::direct_import_backend<int, wrong_route>::context;
    auto endpoint = std::make_shared<context>(std::make_shared<int>());
    auto invalid = portal::async_functions::typed_ping(endpoint);
    (void)invalid;
#elif NEGATIVE_CASE == 8
    tr::router r;
    tr::define_export<portal::export_groups::typed_free_ping, std::int32_t(), wrong_export_route>(
        r, []() {
            return std::int32_t(0);
        });
#elif NEGATIVE_CASE == 9
    tr::router r;
    tr::define_export<portal::export_groups::typed_free_ping, void(), wrong_export_route>(r,
                                                                                          []() {});
#elif NEGATIVE_CASE == 10
    using bad_codec = w::default_codec_t<Bad>;
    (void)sizeof(bad_codec);
#elif NEGATIVE_CASE == 11
    tr::router r;
    tr::define_export<portal::export_groups::typed_free_add,
                      std::int32_t(std::int32_t, std::int32_t), matching_route>(
        r, [](std::int32_t, std::int32_t) {
            return std::int64_t{1};
        });
#elif NEGATIVE_CASE == 12
    tr::router r;
    tr::define_export<portal::export_groups::typed_free_ping, void(), matching_route>(r, [] {
        return 1;
    });
#elif NEGATIVE_CASE == 13
    using invalid = tr::signature_method<1, 1, void(std::int32_t&)>;
    (void)sizeof(invalid);
#endif
}
