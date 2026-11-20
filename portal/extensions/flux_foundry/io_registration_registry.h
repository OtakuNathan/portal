#ifndef PORTAL_FF_IO_REGISTRATION_REGISTRY_H
#define PORTAL_FF_IO_REGISTRATION_REGISTRY_H

#include <portal/rpc/typed.h>
#include <atomic>
#include <cassert>
#include <utility>
#include <list>
#include <thread>

namespace portal {
namespace ff {
// Construct, resolve requests and install updates on the owning I/O thread.
// Join all preparations, queued updates and handler calls before destruction.
class io_registration_registry {
    using router = typed_rpc::router;
    struct batch {
        std::shared_ptr<void> module;
        router::handler_storage
            handlers; // Destroy module-defined handlers before releasing the pin.
    };
    struct staging {
        std::list<batch> batches;
        router::index index;

        template <class Register>
        bool build(const router::index& current, std::shared_ptr<void> module, Register&& define) {
            batches.emplace_back();
            auto& batch = batches.front();
            batch.module = std::move(module);
            router additions;
            if (define(additions) != wire::errc::ok)
                return false;
            if (additions.size() == 0) {
                batches.clear();
                return true;
            }
            if (current.extend(additions, index) != wire::errc::ok)
                return false;
            batch.handlers = std::move(additions).take_handlers();
            return true;
        }
    };
    const std::thread::id io_thread_ = std::this_thread::get_id();
    std::list<batch> owners_;
    router::index current_;
    std::atomic_flag preparing_ = ATOMIC_FLAG_INIT;

public:
    enum class status { ok, busy, rejected, wrong_thread, not_delivered };
    class update {
        friend class io_registration_registry;
        io_registration_registry* owner_;
        std::unique_ptr<staging> next_;
        status result_;
        explicit update(io_registration_registry* owner, status result) noexcept
            : owner_(owner), result_(result) {}
        void release() noexcept {
            next_.reset();
            if (owner_)
                std::exchange(owner_, nullptr)->preparing_.clear(std::memory_order_release);
        }

    public:
        update(update&& other) noexcept
            : owner_(std::exchange(other.owner_, nullptr)), next_(std::move(other.next_)),
              result_(std::exchange(other.result_, status::rejected)) {}
        ~update() {
            release();
        }
        status result() const noexcept {
            return result_;
        }
        status install() && noexcept {
            if (result_ != status::ok)
                return result_;
            if (!owner_)
                return status::rejected;
            if (owner_->io_thread_ != std::this_thread::get_id()) {
                result_ = status::wrong_thread;
            } else if (!next_->batches.empty()) {
                owner_->owners_.splice(owner_->owners_.end(), next_->batches);
                owner_->current_ = std::move(next_->index);
            }
            release();
            return result_;
        }
    };

    template <class Register>
    update prepare(std::shared_ptr<void> module, Register&& define) {
        if (preparing_.test_and_set(std::memory_order_acquire))
            return update(nullptr, status::busy);
        update job(this, status::ok);
        job.next_ = std::make_unique<staging>();
        if (!job.next_->build(current_, std::move(module), std::forward<Register>(define))) {
            job.result_ = status::rejected;
            job.release();
        }
        return job;
    }
    router::resolved_request resolve(wire::view frame, wire::limits l = {}) const noexcept {
        assert(io_thread_ == std::this_thread::get_id());
        return current_.resolve(frame, l);
    }
    wire::errc dispatch(const wire::bytes& frame, wire::bytes& reply, wire::limits l = {}) const {
        return resolve(wire::view(frame), l).dispatch_into(reply);
    }
};
} // namespace ff
} // namespace portal
#endif
