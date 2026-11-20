#include "fixture.h"
#include "portal/rpc.h"

#define PORTAL_IMPORT_DEF "tests/typed_rpc/import.def"
#define PORTAL_EXPORT_DEF "tests/typed_rpc/export.def"
#include "portal/portal.h"
#undef PORTAL_IMPORT_DEF
#undef PORTAL_EXPORT_DEF

namespace portal {
namespace typed_native {
class service_impl {
public:
    explicit service_impl(std::int32_t bias) noexcept : bias_(bias) {}

    std::string echo(const std::string& input) const {
        return input;
    }

    std::int32_t echo(std::int32_t a, std::int32_t b) const noexcept {
        return a + b + bias_;
    }

    Packet roundtrip_packet(const Packet& value) const {
        return value;
    }

private:
    std::int32_t bias_ = 0;
};
} // namespace typed_native
} // namespace portal

namespace {
using DefaultAdd = tr::signature_method<0x101, 1, std::int32_t(std::int32_t, std::int32_t)>;
using DefaultEcho = tr::signature_method<0x103, 1, std::string(const std::string&)>;
using DefaultPacket = tr::signature_method<0x104, 1, Packet(const Packet&)>;
using DefaultPing = tr::signature_method<0x105, 1, void()>;

static_assert(std::is_same<DefaultAdd, Add>::value,
              "signature_method must match explicit fixed-width method");
static_assert(DefaultAdd::fingerprint() == Add::fingerprint(),
              "signature_method fingerprint must match explicit method");
static_assert(std::is_same<DefaultEcho, Echo>::value,
              "default string codec must match default 1 MiB blob");
static_assert(std::is_same<DefaultPacket, EchoPacket>::value,
              "PORTAL_WIRE_RECORD types must project to wire::record<T>");
static_assert(std::is_same<DefaultPing, Ping>::value, "void return must project to wire::none");
static_assert(std::is_same<w::default_codec_t<std::uint8_t>, w::u8>::value, "u8 default");
static_assert(std::is_same<w::default_codec_t<std::uint16_t>, w::u16>::value, "u16 default");
static_assert(std::is_same<w::default_codec_t<std::uint32_t>, w::u32>::value, "u32 default");
static_assert(std::is_same<w::default_codec_t<std::uint64_t>, w::u64>::value, "u64 default");
static_assert(std::is_same<w::default_codec_t<std::int8_t>, w::i8>::value, "i8 default");
static_assert(std::is_same<w::default_codec_t<std::int16_t>, w::i16>::value, "i16 default");
static_assert(std::is_same<w::default_codec_t<std::int64_t>, w::i64>::value, "i64 default");
static_assert(std::is_same<w::default_codec_t<bool>, w::boolean>::value, "bool default");
static_assert(std::is_same<w::default_codec_t<double>, w::f64>::value, "double default");
static_assert(
    std::is_same<w::default_codec_t<std::array<std::uint16_t, 3>>, w::array<w::u16, 3>>::value,
    "array default");
static_assert(std::is_same<w::default_codec_t<std::vector<std::uint32_t>>,
                           w::sequence<w::u32, w::default_sequence_limit>>::value,
              "vector default");
static_assert(std::is_same<w::default_codec_t<w::option<double>>, w::maybe<w::f64>>::value,
              "option default");

template <class Symbol, class Signature>
struct server_routes;

template <>
struct server_routes<portal::export_groups::typed_free_add,
                     std::int32_t(std::int32_t, std::int32_t)> {
    using type = DefaultAdd;
};
template <>
struct server_routes<portal::export_groups::typed_free_add, std::string(const std::string&)> {
    using type = DefaultEcho;
};
template <>
struct server_routes<portal::export_groups::typed_free_ping, void()> {
    using type = DefaultPing;
};

template <>
struct server_routes<portal::exports::service_impl::echo_member, std::string(const std::string&)> {
    using type = DefaultEcho;
};
template <>
struct server_routes<portal::exports::service_impl::echo_member,
                     std::int32_t(std::int32_t, std::int32_t)> {
    using type = DefaultAdd;
};
template <>
struct server_routes<portal::exports::service_impl::roundtrip_packet_member,
                     Packet(const Packet&)> {
    using type = DefaultPacket;
};

template <class Symbol, class Signature>
struct client_routes;

template <>
struct client_routes<portal::import_symbols::typed_service::echo, std::string(const std::string&)> {
    using type = DefaultEcho;
};
template <>
struct client_routes<portal::import_symbols::typed_service::echo,
                     std::int32_t(std::int32_t, std::int32_t)> {
    using type = DefaultAdd;
};
template <>
struct client_routes<portal::import_symbols::typed_ping, void()> {
    using type = DefaultPing;
};

std::int32_t free_add(std::int32_t a, std::int32_t b) noexcept {
    return a + b;
}

struct direct_transport {
    tr::router* server = nullptr;

    template <class Method, class H>
    portal::async_submission async_round_trip_into(tr::prepared_request<Method> request,
                                                   H done) noexcept {
        w::bytes frame(request.size());
        w::bytes reply;
        w::writer writer(frame.data(), frame.size());
        CHECK(request.write(writer) == w::errc::ok);
        server->dispatch_into(frame, reply);
        done(std::move(reply), {});
        return portal::async_submission::accept();
    }
};

void check_free_exports() {
    tr::router server;
    int ping_calls = 0;
    CHECK(tr::define_export<portal::export_groups::typed_free_add, server_routes>(
              server, &free_add) == w::errc::ok);
    CHECK(tr::define_export<portal::export_groups::typed_free_add, std::string(const std::string&),
                            server_routes>(server, [](const std::string& s) {
              return s;
          }) == w::errc::ok);
    CHECK(tr::define_export<portal::export_groups::typed_free_ping, void(), server_routes>(
              server, [&ping_calls]() noexcept {
                  ++ping_calls;
              }) == w::errc::ok);
    // Duplicate registration of the same method ID/revision is rejected cleanly.
    CHECK(tr::define_export<portal::export_groups::typed_free_add, server_routes>(
              server, &free_add) == w::errc::invalid_value);

    tr::loopback_transport loopback(server);
    tr::workspace storage;
    auto sum = tr::call<DefaultAdd>(loopback, storage, {}, std::int32_t(19), std::int32_t(23));
    CHECK(sum && sum.value == 42);
    auto echoed = tr::call<DefaultEcho>(loopback, storage, {}, std::string("typed-default"));
    CHECK(echoed && echoed.value == "typed-default");
    auto ping = tr::call<DefaultPing>(loopback, storage, {});
    CHECK(ping && ping_calls == 1);
}

void check_class_export_and_def_roundtrip() {
    portal::typed_native::service_impl service(5);
    tr::router server;
    tr::router conflicting;
    CHECK(conflicting.define<DefaultPacket>([](Packet p) {
        return p;
    }) == w::errc::ok);
    CHECK(tr::export_class<portal::exports::service_impl, server_routes>(conflicting, service) ==
          w::errc::invalid_value);
    CHECK(conflicting.size() == 1);
    CHECK(tr::define_export<portal::export_groups::typed_free_ping, void(), server_routes>(
              server, [] {}) == w::errc::ok);
    CHECK(tr::export_class<portal::exports::service_impl, server_routes>(server, service) ==
          w::errc::ok);
    // Duplicate export of the same class reports invalid_value on the first duplicate method.
    CHECK(tr::export_class<portal::exports::service_impl, server_routes>(server, service) ==
          w::errc::invalid_value);

    tr::loopback_transport loopback(server);
    tr::workspace storage;
    auto pkt_in = packet();
    auto pkt_out = tr::call<DefaultPacket>(loopback, storage, {}, pkt_in);
    CHECK(pkt_out && same(pkt_out.value, pkt_in));

    auto io = std::make_shared<direct_transport>();
    io->server = &server;
    using context = tr::direct_import_backend<direct_transport, client_routes>::context;
    using view_type = portal::typed_service<context>;
    auto endpoint = std::make_shared<context>(io);
    auto client = portal::bind_async<portal::typed_service>(endpoint, view_type::object_t{});
    auto erased = portal::bind_erased_async<portal::typed_service>(endpoint, view_type::object_t{});

    int finished = 0;
    auto sum_call = client.echo(std::int32_t(10), std::int32_t(27));
    static_assert(!std::is_same<decltype(sum_call), portal::invocation<std::int32_t>>::value,
                  "bind_async must return concrete typed invocation");
    std::move(sum_call).start([&](portal::async_result<std::int32_t> r) noexcept {
        CHECK(r.has_value() && *r.value == 42);
        ++finished;
    });

    auto echo_call = erased.echo(std::string("via-erased-sideway"));
    static_assert(std::is_same<decltype(echo_call), portal::invocation<std::string>>::value,
                  "bind_erased_async must return erased invocation");
    std::move(echo_call).start([&](portal::async_result<std::string> r) noexcept {
        CHECK(r.has_value() && *r.value == "via-erased-sideway");
        ++finished;
    });
    CHECK(finished == 2);
}
} // namespace

int main() {
    check_free_exports();
    check_class_export_and_def_roundtrip();
    return 0;
}
