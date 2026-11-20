#include <cstddef>
#include "fixture.h"
#include "portal/rpc/typed_import.h"
#define PORTAL_IMPORT_DEF "tests/typed_rpc/import.def"
#include "portal/import.h"
#undef PORTAL_IMPORT_DEF
#include "benchmarks/rpc_ring_ff.h"

namespace rb = rpc_ring_bench;
namespace ff = flux_foundry;

static_assert(alignof(rb::byte_ring) <= alignof(std::max_align_t),
              "the send ring must support ordinary C++14 heap allocation");

static void pad(rb::byte_ring& ring, std::size_t size) {
    w::bytes filler(size, 0xaa);
    CHECK(ring.try_write(size, [&](auto& writer) {
        writer.append(filler.data(), size);
    }) == rb::write_status::published);
}
static w::bytes drain(rb::byte_ring& ring) {
    w::bytes frame;
    while (ring.read([&](const std::uint8_t* data, std::size_t count) {
        frame.insert(frame.end(), data, data + count);
        return count;
    })) {
    }
    return frame;
}

struct short_codec : w::u32 {
    static bool write(w::writer& writer, std::uint32_t number) noexcept {
        return writer.integer(static_cast<std::uint16_t>(number));
    }
};

static void check_reservations() {
    auto input = packet();
    w::bytes expected;
    CHECK(tr::encode_request<EchoPacket>(expected, {}, input) == w::errc::ok);
    tr::prepared_request<EchoPacket> request({}, std::move(input));
    CHECK(request.size() == expected.size());
    for (std::size_t offset = 0; offset < 128; ++offset) {
        rb::byte_ring ring(128, 128);
        pad(ring, offset);
        CHECK(drain(ring).size() == offset);
        using broken = tr::method<42, 1, w::none, short_codec>;
        tr::prepared_request<broken> invalid({}, std::uint32_t(123));
        CHECK(ring.try_write_typed(invalid) == rb::write_status::invalid);
        CHECK(drain(ring).empty());
        CHECK(ring.try_write_typed(request) == rb::write_status::published);
        CHECK(drain(ring) == expected);
    }
    std::uint8_t first = 1, second = 2;
    w::writer tiny(&first, 1, &second, 1);
    CHECK(!tiny.integer(std::uint32_t(0)));
    CHECK(first == 1 && second == 2);
    CHECK(tiny.integer(std::uint16_t(0xbeef)) && tiny.empty());
    CHECK(first == 0xef && second == 0xbe);
}

struct transport {
    rb::byte_ring ring{128, 64};
    tr::router server;
    decltype(ff::make_blueprint<rb::space_request, portal::async_error>() |
             ff::await<rb::ring_space_awaitable>() | ff::end()) blueprint =
        ff::make_blueprint<rb::space_request, portal::async_error>() |
        ff::await<rb::ring_space_awaitable>() | ff::end();
    unsigned submissions = 0;
    transport() {
        bind(server);
    }

    template <class Method, class H>
    portal::async_submission async_round_trip_into(tr::prepared_request<Method> request, H done) {
        ++submissions;
        rb::async_write_typed(
            ring, blueprint, std::move(request), [this, done](portal::async_error e) noexcept {
                w::bytes reply;
                if (!e.code && !e.exception)
                    CHECK(server.dispatch_into(drain(ring), reply) == w::errc::ok);
                done(std::move(reply), std::move(e));
            });
        return portal::async_submission::accept();
    }
};

template <class Symbol, class Signature>
struct routes;
template <>
struct routes<portal::import_symbols::typed_service::echo, std::string(const std::string&)> {
    using type = Echo;
};

static void check_async(bool erased, bool stop) {
    auto t = std::make_shared<transport>();
    pad(t->ring, 123);
    const std::string expected(57, 'a');
    std::string source = expected;
    using context = tr::direct_import_backend<transport, routes>::context;
    using service = portal::typed_service<context>;
    auto endpoint = std::make_shared<context>(t);
    unsigned count = 0;
    auto handler = [&](portal::async_result<std::string> r) noexcept {
        if (stop)
            CHECK(!r.has_value() && r.error.code == std::errc::operation_canceled);
        else
            CHECK(r.has_value() && *r.value == expected);
        ++count;
    };
    auto start = [&](const auto& client) {
        auto call = client.echo(source);
        source.assign(57, 'b');
        std::move(call).start(handler);
    };
    if (erased)
        start(portal::bind_erased_async<portal::typed_service>(endpoint, service::object_t{}));
    else
        start(portal::bind_async<portal::typed_service>(endpoint, service::object_t{}));
    endpoint.reset();
    CHECK(count == 0 && t->ring.producer_paused());
    if (stop)
        t->ring.stop_async_waits();
    CHECK(drain(t->ring) == w::bytes(123, 0xaa));
    CHECK(count == 1 && !t->ring.producer_paused() && t->submissions == 1);
    t->ring.stop_async_waits();
    CHECK(count == 1);
}

int main() {
    check_reservations();
    for (bool erased : {false, true})
        for (bool stop : {false, true})
            check_async(erased, stop);

    auto t = std::make_shared<transport>();
    unsigned failures = 0;
    auto oversized = tr::make_direct_rpc_invocation<Echo>(t, {}, std::string(128, 'x'));
    std::move(oversized).start([&](portal::async_result<std::string> r) noexcept {
        CHECK(!r.has_value() && r.error.code == std::errc::message_size);
        ++failures;
    });
    w::limits budget;
    budget.max_frame_bytes = 32;
    auto limited = tr::make_direct_rpc_invocation<Echo>(t, budget, std::string("large"));
    std::move(limited).start([&](portal::async_result<std::string> r) noexcept {
        CHECK(!r.has_value() && r.error.code == tr::make_error_code(w::errc::limit_exceeded));
        ++failures;
    });
    CHECK(t->submissions == 1 && failures == 2 && drain(t->ring).empty());
}
