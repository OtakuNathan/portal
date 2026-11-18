// A trusted local translation unit compiled by the registration test and probe.
#include <portal/rpc/typed.h>
#include <atomic>

namespace rpc = portal::typed_rpc;
namespace w = portal::wire;
using method = rpc::method<100, 1, w::i32, w::i32, w::i32>;

struct lifetime {
    std::atomic<unsigned>* destroyed;
    ~lifetime() { if (destroyed) ++*destroyed; }
};

extern "C" w::errc portal_register_rpc_v1(rpc::router& router, void* context) noexcept {
    try {
        auto guard = std::shared_ptr<lifetime>(new lifetime{static_cast<std::atomic<unsigned>*>(context)});
        return router.define<method>([guard](std::int32_t a, std::int32_t b) -> std::int32_t {
            auto sum = std::int64_t(a) + b + 100;
            return static_cast<std::int32_t>(std::max<std::int64_t>(INT32_MIN,
                std::min<std::int64_t>(INT32_MAX, sum)));
        });
    } catch (...) { return w::errc::handler_failed; }
}
