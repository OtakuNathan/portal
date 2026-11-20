#ifndef PORTAL_RPC_TYPED_H
#define PORTAL_RPC_TYPED_H
#include "../wire/schema.h"
#include <algorithm>
#include <memory>
#include <new>
#include <stdexcept>

namespace portal {
namespace typed_rpc {
using wire::bytes;
using wire::errc;
using wire::limits;
using wire::view;
constexpr std::size_t header_size = 32;
constexpr std::uint32_t request_magic = 0x31525450u;  // PTR1
constexpr std::uint32_t response_magic = 0x31535450u; // PTS1
constexpr std::uint16_t protocol_version = 1;

namespace detail {
template <class... C>
struct codec_meta;
template <>
struct codec_meta<> {
    static constexpr std::uint64_t hash(std::uint64_t h) noexcept {
        return h;
    }
};
template <class C, class... Rest>
struct codec_meta<C, Rest...> {
    static constexpr std::uint64_t hash(std::uint64_t h) noexcept {
        return codec_meta<Rest...>::hash(wire::mix(h, C::fingerprint()));
    }
};
template <class C>
struct cpp_return {
    using type = typename C::value_type;
};
template <>
struct cpp_return<wire::none> {
    using type = void;
};
template <class R, class... A>
struct signature {
    using type = R(A...);
};
template <class C>
struct invoke_result {
    template <class F, class... A>
    static typename C::value_type call(const F& f, A&&... a) {
        static_assert(
            std::is_same<decltype(f(std::forward<A>(a)...)), typename C::value_type>::value,
            "typed RPC handler must return the declared type exactly");
        return f(std::forward<A>(a)...);
    }
};
template <>
struct invoke_result<wire::none> {
    template <class F, class... A>
    static wire::unit call(const F& f, A&&... a) {
        static_assert(std::is_void<decltype(f(std::forward<A>(a)...))>::value,
                      "none codec requires a void-returning handler");
        f(std::forward<A>(a)...);
        return {};
    }
};
} // namespace detail

// A single explicit method description projects both a C++ signature and a
// wire codec. ID/revision are deployed protocol identifiers, not source lines.
template <std::uint64_t Id, std::uint32_t Revision, class Return, class... Arguments>
struct method {
    static_assert(Id && Revision, "method ID and revision must be nonzero");
    using return_codec = Return;
    using result_type = typename Return::value_type;
    using arguments = std::tuple<typename Arguments::value_type...>;
    using signature_t = typename detail::signature<typename detail::cpp_return<Return>::type,
                                                   typename Arguments::value_type...>::type;
    static constexpr std::uint64_t id = Id;
    static constexpr std::uint32_t revision = Revision;
    static constexpr std::size_t arity = sizeof...(Arguments);
    static constexpr std::uint64_t fingerprint() noexcept {
        return detail::codec_meta<Arguments...>::hash(
            wire::mix(wire::mix(wire::hash_seed, Return::fingerprint()), arity));
    }
    template <class Tuple, std::size_t... I>
    static bool size_args(wire::measure& s, const Tuple& a, std::index_sequence<I...>) noexcept {
        bool ok = true;
        using swallow = int[];
        (void)swallow{0, (ok = ok && Arguments::size(s, std::get<I>(a)), 0)...};
        return ok;
    }
    template <class Tuple, std::size_t... I>
    static bool write_args(wire::writer& w, const Tuple& a, std::index_sequence<I...>) noexcept {
        bool ok = true;
        using swallow = int[];
        (void)swallow{0, (ok = ok && Arguments::write(w, std::get<I>(a)), 0)...};
        return ok;
    }
    template <std::size_t... I>
    static bool read_args(wire::reader& r, arguments& a, std::index_sequence<I...>) {
        bool ok = true;
        using swallow = int[];
        (void)swallow{0, (ok = ok && Arguments::read(r, std::get<I>(a)), 0)...};
        return ok;
    }
};
// ODR-use through references/forwarding must also link in C++14.
template <std::uint64_t Id, std::uint32_t Revision, class Return, class... Arguments>
constexpr std::uint64_t method<Id, Revision, Return, Arguments...>::id;
template <std::uint64_t Id, std::uint32_t Revision, class Return, class... Arguments>
constexpr std::uint32_t method<Id, Revision, Return, Arguments...>::revision;
template <std::uint64_t Id, std::uint32_t Revision, class Return, class... Arguments>
constexpr std::size_t method<Id, Revision, Return, Arguments...>::arity;
struct envelope {
    std::uint32_t magic = 0;
    std::uint16_t version = 0;
    errc status = errc::ok;
    std::uint64_t method_id = 0;
    std::uint32_t revision = 0;
    std::uint32_t payload_size = 0;
    std::uint64_t fingerprint = 0;
    view payload;
};
inline errc read_envelope(view frame, std::uint32_t magic, envelope& out, limits l = {}) noexcept {
    if (frame.size > l.max_frame_bytes)
        return errc::limit_exceeded;
    if (frame.size < header_size || !frame.data)
        return errc::malformed;
    envelope staged;
    wire::reader r(frame, l);
    std::uint16_t status = 0;
    if (!r.integer(staged.magic) || !r.integer(staged.version) || !r.integer(status) ||
        !r.integer(staged.method_id) || !r.integer(staged.revision) ||
        !r.integer(staged.payload_size) || !r.integer(staged.fingerprint))
        return errc::malformed;
    if (staged.magic != magic || staged.version != protocol_version ||
        status > static_cast<std::uint16_t>(errc::transport_failed) ||
        staged.payload_size != frame.size - header_size ||
        (magic == request_magic && status != 0) || (status != 0 && staged.payload_size != 0))
        return errc::malformed;
    staged.status = static_cast<errc>(status);
    staged.payload = view(frame.data + header_size, staged.payload_size);
    out = staged;
    return errc::ok;
}
inline bool write_header(wire::writer& w, std::uint32_t magic, std::uint64_t id,
                         std::uint32_t revision, std::uint64_t hash, std::uint32_t payload,
                         errc status) noexcept {
    return w.integer(magic) && w.integer(protocol_version) &&
           w.integer(static_cast<std::uint16_t>(status)) && w.integer(id) && w.integer(revision) &&
           w.integer(payload) && w.integer(hash);
}
inline errc error_reply(bytes& out, const envelope& e, errc status, limits l = {}) {
    if (l.max_frame_bytes < header_size) {
        out.clear();
        return errc::limit_exceeded;
    }
    out.resize(header_size);
    wire::writer w(out.data(), out.size());
    (void)write_header(w, response_magic, e.method_id, e.revision, e.fingerprint, 0, status);
    return status;
}
namespace detail {
template <class Method, class... A>
wire::measure measure_request(limits l, const A&... args) noexcept {
    static_assert(
        std::is_same<typename Method::arguments,
                     std::tuple<typename std::decay<A>::type...>>::value,
        "typed RPC arguments must match the declared value types; make narrowing explicit");
    wire::measure s((std::min)(
        l.max_frame_bytes, static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)())));
    if (s.add(header_size) &&
        !Method::size_args(s, std::tie(args...), std::make_index_sequence<Method::arity>{}) &&
        s.error() == errc::ok)
        s.fail(errc::invalid_value);
    return s;
}

template <class Method, class Tuple>
errc write_request(wire::writer& w, std::size_t size, const Tuple& args) noexcept {
    const auto payload = static_cast<std::uint32_t>(size - header_size);
    if (!write_header(w, request_magic, Method::id, Method::revision, Method::fingerprint(),
                      payload, errc::ok) ||
        !Method::write_args(w, args, std::make_index_sequence<Method::arity>{}) || !w.empty())
        return errc::invalid_value;
    return errc::ok;
}
} // namespace detail
template <class Method, class... A>
errc encode_request(bytes& out, limits l, const A&... a) {
    const auto s = detail::measure_request<Method>(l, a...);
    if (s.error() != errc::ok)
        return s.error();
    out.resize(s.size());
    wire::writer w(out.data(), out.size());
    const auto status = detail::write_request<Method>(w, s.size(), std::tie(a...));
    if (status != errc::ok)
        out.clear();
    return status;
}
template <class Method>
errc encode_response(bytes& out, const typename Method::result_type& result, limits l = {}) {
    using C = typename Method::return_codec;
    wire::measure s((std::min)(
        l.max_frame_bytes, static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)())));
    if (!s.add(header_size) || !C::size(s, result))
        return s.error();
    out.resize(s.size());
    wire::writer w(out.data(), out.size());
    if (!write_header(w, response_magic, Method::id, Method::revision, Method::fingerprint(),
                      static_cast<std::uint32_t>(s.size() - header_size), errc::ok) ||
        !C::write(w, result) || !w.empty()) {
        out.clear();
        return errc::invalid_value;
    }
    return errc::ok;
}
template <class Method>
wire::result<typename Method::result_type> decode_response(view frame, limits l = {}) {
    wire::result<typename Method::result_type> result;
    envelope e;
    result.error = read_envelope(frame, response_magic, e, l);
    if (!result)
        return result;
    if (e.method_id != Method::id || e.revision != Method::revision ||
        e.fingerprint != Method::fingerprint()) {
        result.error = errc::incompatible_schema;
        return result;
    }
    if (e.status != errc::ok) {
        result.error = e.status;
        return result;
    }
    return wire::decode<typename Method::return_codec>(e.payload, l);
}

// Immutable routing metadata after registration. Registered handlers must be
// const-invocable and safe for the chosen concurrency. No per-request
// std::function/vector<value> is built.
class router {
    struct entry_base {
        virtual ~entry_base() = default;
        virtual errc invoke(view, bytes&, limits) const = 0;
    };
    template <class Method, class F>
    struct entry final : entry_base {
        F function;
        explicit entry(F f) : function(std::move(f)) {}
        template <std::size_t... I>
        typename Method::result_type execute(typename Method::arguments& args,
                                             std::index_sequence<I...>) const {
            return detail::invoke_result<typename Method::return_codec>::call(
                function, std::move(std::get<I>(args))...);
        }
        errc invoke(view body, bytes& out, limits l) const override {
            typename Method::arguments args{};
            wire::reader r(body, l);
            using indices = std::make_index_sequence<Method::arity>;
            // Decode owns its payloads. Full consumption is checked before any
            // business call; even same-vector input/output is safe afterwards.
            if (!Method::read_args(r, args, indices{}) || !r.empty())
                return r.error() == errc::ok ? errc::malformed : r.error();
            auto result = execute(args, indices{});
            return encode_response<Method>(out, result, l);
        }
    };
    struct route {
        std::uint64_t id, hash;
        std::uint32_t revision;
        std::unique_ptr<entry_base> implementation;
    };
    std::vector<route> routes_;
    using key_type = std::pair<std::uint64_t, std::uint32_t>;
    template <class Row>
    static key_type key(const Row& row) noexcept {
        return {row.id, row.revision};
    }
    template <class Row>
    static bool before(const Row& row, key_type wanted) noexcept {
        return key(row) < wanted;
    }

public:
    // Borrows the request bytes and the separately owned handler. No index row escapes lookup.
    class resolved_request {
        friend class router;
        envelope header_;
        limits limits_;
        const entry_base* handler_ = nullptr;
        errc status_ = errc::ok;

    public:
        errc dispatch_into(bytes& reply) const {
            auto error = status_;
            if (handler_) {
#if defined(__cpp_exceptions) || defined(__EXCEPTIONS) || defined(_CPPUNWIND)
                try {
#endif
                    error = handler_->invoke(header_.payload, reply, limits_);
#if defined(__cpp_exceptions) || defined(__EXCEPTIONS) || defined(_CPPUNWIND)
                } catch (const std::bad_alloc&) {
                    throw;
                } catch (...) {
                    error = errc::handler_failed;
                }
#endif
            }
            return error == errc::ok ? error : error_reply(reply, header_, error, limits_);
        }

    private:
        resolved_request() = default;
    };

private:
    static const entry_base* pointer(const std::unique_ptr<entry_base>& p) noexcept {
        return p.get();
    }
    static const entry_base* pointer(const entry_base* p) noexcept {
        return p;
    }
    template <class Row>
    static resolved_request lookup(const std::vector<Row>& rows, view frame, limits l) noexcept {
        resolved_request result;
        result.limits_ = l;
        auto& e = result.header_;
        result.status_ = read_envelope(frame, request_magic, e, l);
        if (result.status_ != errc::ok)
            return result;
        const auto wanted = std::make_pair(e.method_id, e.revision);
        auto pos = std::lower_bound(rows.begin(), rows.end(), wanted, before<Row>);
        if (pos == rows.end() || key(*pos) != wanted)
            result.status_ = errc::unknown_method;
        else if (pos->hash != e.fingerprint)
            result.status_ = errc::incompatible_schema;
        else
            result.handler_ = pointer(pos->implementation);
        return result;
    }

public:
    class handler_storage {
        friend class router;
        std::vector<std::unique_ptr<entry_base>> entries_;
    };
    // Transfer ownership of the stable handler objects out of a temporary registration table.
    handler_storage take_handlers() && {
        handler_storage storage;
        storage.entries_.reserve(routes_.size());
        for (auto& row : routes_)
            storage.entries_.push_back(std::move(row.implementation));
        routes_.clear();
        return storage;
    }
    // Borrowed index. Keep each contributing router or its transferred handler storage alive.
    class index {
        struct binding {
            std::uint64_t id, hash;
            std::uint32_t revision;
            const entry_base* implementation;
        };
        std::vector<binding> rows_;

    public:
        resolved_request resolve(view frame, limits l = {}) const noexcept {
            return lookup(rows_, frame, l);
        }
        // Merge two sorted tables into a fresh index. A duplicate rejects the whole batch.
        errc extend(const router& added, index& output) const {
            index next;
            if (added.size() > next.rows_.max_size() - rows_.size())
                return errc::limit_exceeded;
            next.rows_.reserve(rows_.size() + added.size());
            auto old = rows_.begin();
            for (const auto& r : added.routes_) {
                while (old != rows_.end() && key(*old) < key(r))
                    next.rows_.push_back(*old++);
                if (old != rows_.end() && key(*old) == key(r))
                    return errc::invalid_value;
                next.rows_.push_back({r.id, r.hash, r.revision, r.implementation.get()});
            }
            next.rows_.insert(next.rows_.end(), old, rows_.end());
            output = std::move(next);
            return errc::ok;
        }
    };
    template <class Method, class F>
    errc define(F&& fn) {
        const auto key = std::make_pair(static_cast<std::uint64_t>(Method::id),
                                        static_cast<std::uint32_t>(Method::revision));
        const auto pos = std::lower_bound(routes_.begin(), routes_.end(), key, before<route>);
        if (pos != routes_.end() && pos->id == key.first && pos->revision == key.second)
            return errc::invalid_value; // explicit duplicate rejection; no dangling cached route
        using E = entry<Method, typename std::decay<F>::type>;
        std::unique_ptr<entry_base> impl(new E(std::forward<F>(fn)));
        route r{Method::id, Method::fingerprint(), Method::revision, std::move(impl)};
        routes_.insert(pos, std::move(r));
        return errc::ok;
    }
    errc dispatch_into(const bytes& frame, bytes& reply, limits l = {}) const {
        return lookup(routes_, view(frame), l).dispatch_into(reply);
    }
    std::size_t size() const noexcept {
        return routes_.size();
    }
};

// A bounded idle cache, not a session/in-flight memory bound. One workspace per
// worker. Every active/reentrant call owns separate vectors until it returns.
class workspace {
    bytes request_, response_;
    std::size_t max_retained_;
    static void recycle(bytes& active, bytes& cache, std::size_t limit) noexcept {
        // Keep initialized byte elements so a same-size frame is overwritten
        // without zero-filling it first. Every encoder overwrites the entire
        // published size; this is not a borrowed payload or a secure wipe.
        if (active.capacity() <= limit && active.capacity() > cache.capacity())
            active.swap(cache);
    }

public:
    explicit workspace(std::size_t max_retained = 64 * 1024) noexcept
        : max_retained_(max_retained) {}
    workspace(const workspace&) = delete;
    workspace& operator=(const workspace&) = delete;
    void release_cached_storage() noexcept {
        bytes{}.swap(request_);
        bytes{}.swap(response_);
    }
    std::size_t retained_capacity() const noexcept {
        return request_.capacity() + response_.capacity();
    }
    struct scope {
        workspace& owner;
        bytes request, response;
        explicit scope(workspace& w) noexcept : owner(w) {
            request.swap(w.request_);
            response.swap(w.response_);
        }
        scope(const scope&) = delete;
        scope& operator=(const scope&) = delete;
        ~scope() {
            recycle(request, owner.request_, owner.max_retained_);
            recycle(response, owner.response_, owner.max_retained_);
        }
    };
};
class loopback_transport {
    const router* router_;
    limits limits_;

public:
    explicit loopback_transport(const router& r, limits l = {}) noexcept
        : router_(&r), limits_(l) {}
    void round_trip_into(const bytes& request, bytes& response) const {
        (void)router_->dispatch_into(request, response, limits_);
    }
};
template <class Method, class Transport, class... A>
wire::result<typename Method::result_type> call(Transport& transport, workspace& storage, limits l,
                                                const A&... a) {
    workspace::scope buffers(storage);
    wire::result<typename Method::result_type> result;
    result.error = encode_request<Method>(buffers.request, l, a...);
    if (!result)
        return result;
    transport.round_trip_into(buffers.request, buffers.response);
    return decode_response<Method>(view(buffers.response), l);
}
} // namespace typed_rpc
} // namespace portal
#endif
