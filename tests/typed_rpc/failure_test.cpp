#include "fixture.h"
#include <exception>
#include <new>

namespace {
constexpr std::size_t payload_bytes = 1024;
thread_local long countdown = -1;
thread_local bool injected = false;
thread_local bool measuring = false;
thread_local std::size_t allocations = 0;

bool reject_allocation(std::size_t size) noexcept {
    // MSVC Debug allocates small iterator proxies even inside noexcept string
    // and vector constructors. Inject into RPC frame/payload storage only.
    if (size < payload_bytes)
        return false;
    if (measuring)
        ++allocations;
    if (countdown < 0)
        return false;
    if (countdown-- == 0) {
        countdown = -1; // One failure; unwinding and runtime bookkeeping may allocate.
        injected = true;
        return true;
    }
    return false;
}
} // namespace

void* operator new(std::size_t size) {
    if (reject_allocation(size))
        throw std::bad_alloc();
    if (void* p = std::malloc(size ? size : 1))
        return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void operator delete(void* p) noexcept {
    std::free(p);
}
void operator delete[](void* p) noexcept {
    std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
    std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
    std::free(p);
}

int main() {
    // Unexpected noexcept failures must fail promptly instead of opening a
    // Windows CRT termination dialog and waiting for CTest's timeout.
    std::set_terminate([]() noexcept {
        countdown = -1;
        std::fputs("unexpected termination during allocation-failure injection\n", stderr);
        std::fflush(stderr);
        std::_Exit(EXIT_FAILURE);
    });

    // Exercise the proxy-sized allocation exclusion on every STL, including
    // implementations whose Debug containers do not allocate iterator proxies.
    countdown = 0;
    void* small = ::operator new(2 * sizeof(void*));
    ::operator delete(small);
    CHECK(countdown == 0 && !injected);
    bool caught = false;
    try {
        void* payload = ::operator new(payload_bytes);
        ::operator delete(payload);
    } catch (const std::bad_alloc&) {
        caught = true;
    }
    CHECK(caught && injected && countdown == -1);

    tr::router r;
    bind(r);
    tr::loopback_transport t(r);
    const std::string input(payload_bytes, 'a');
    {
        tr::workspace cache;
        measuring = true;
        auto result = tr::call<Echo>(t, cache, {}, input);
        measuring = false;
        CHECK(result && result.value == input);
    }
    // Request frame, decoded argument, response frame and decoded result must
    // all participate. Measure the actual count instead of assuming STL internals.
    CHECK(allocations >= 4 && allocations < 32);
    std::size_t failed = 0, succeeded = 0;
    for (std::size_t point = 0; point <= allocations; ++point) {
        tr::workspace cache;
        injected = false;
        countdown = static_cast<long>(point);
        try {
            auto result = tr::call<Echo>(t, cache, {}, input);
            countdown = -1;
            CHECK(!injected && result && result.value == input);
            ++succeeded;
        } catch (const std::bad_alloc&) {
            countdown = -1;
            CHECK(injected);
            ++failed;
        }
        // An exception must not poison/reuse live storage in the next call.
        auto next = tr::call<Echo>(t, cache, {}, input);
        CHECK(next && next.value == input);
    }
    CHECK(failed == allocations && succeeded == 1);

    // Full decode before dispatch: valid first payload, invalid second argument.
    using Two = tr::method<0x201, 1, w::i32, w::blob<2048>, w::boolean>;
    int calls = 0;
    CHECK(r.define<Two>([&](std::string, bool) {
        ++calls;
        return std::int32_t(1);
    }) == w::errc::ok);
    w::bytes req, reply;
    CHECK(tr::encode_request<Two>(req, {}, input, true) == w::errc::ok);
    req.back() = 2;
    CHECK(r.dispatch_into(req, reply) == w::errc::invalid_value);
    CHECK(calls == 0);
    auto response = tr::decode_response<Two>(w::view(reply));
    CHECK(!response && response.error == w::errc::invalid_value);
    std::printf("allocation failure and pre-dispatch validation: passed (%zu failure points)\n",
                failed);
}
