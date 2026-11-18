#ifndef PORTAL_FF_REGISTRATION_REGISTRY_H
#define PORTAL_FF_REGISTRATION_REGISTRY_H

#include <portal/rpc/typed.h>
#include <lease.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace portal { namespace ff {
class registration_registry {
    using router = typed_rpc::router;
    using writer_type = lease::access::exclusive_access<router, lease::access::shared>;

    // Router entries can contain destructors implemented in a loaded module.
    std::vector<std::shared_ptr<void>> pins_;
    router router_;
    writer_type writer_;
    std::optional<writer_type::ro_type> reader_;
    std::mutex mutex_;
    std::condition_variable changed_;
    // Closing a slot and admitting a call on it are ordered by the same atomic.
    // A count also accommodates nested calls and threads mapped to the same slot.
    static constexpr std::size_t closed = std::size_t{1} << (std::numeric_limits<std::size_t>::digits - 1);
    struct alignas(64) slot { std::atomic<std::size_t> state{0}; };
    static constexpr std::size_t slot_count = 16;
    slot slots_[slot_count];
    bool updating_ = false;
    std::size_t waiting_ = 0;

    static std::size_t thread_slot() noexcept {
        static std::atomic<std::size_t> next{0};
        static thread_local const auto index = next.fetch_add(1, std::memory_order_relaxed) % slot_count;
        return index;
    }
    std::size_t active_count() const noexcept {
        std::size_t count = 0;
        for (const auto& s : slots_)
            count += s.state.load(std::memory_order_acquire) & ~closed;
        return count;
    }

    struct scope {
        const registration_registry* owner;
        scope* previous;
        static scope*& top() noexcept {
            static thread_local scope* current = nullptr;
            return current;
        }
        explicit scope(const registration_registry* registry) noexcept
            : owner(registry), previous(top()) { top() = this; }
        ~scope() { top() = previous; }
        static bool contains(const registration_registry* registry) noexcept {
            for (auto p = top(); p; p = p->previous)
                if (p->owner == registry) return true;
            return false;
        }
    };

    static bool try_enter(slot& s) noexcept {
        auto state = s.state.load(std::memory_order_relaxed);
        while (!(state & closed)) {
            if (s.state.compare_exchange_weak(state, state + 1, std::memory_order_acquire,
                                             std::memory_order_relaxed)) return true;
        }
        return false;
    }
    bool enter(slot& s) {
        if (try_enter(s)) return true;
        if (scope::contains(this)) return false;
        std::unique_lock<std::mutex> lock(mutex_);
        ++waiting_;
        changed_.wait(lock, [&] { return try_enter(s); });
        --waiting_;
        return true;
    }
    struct active_call {
        registration_registry& owner;
        slot& admitted;
        ~active_call() {
            if (admitted.state.fetch_sub(1, std::memory_order_release) == closed + 1) {
                // Pair with the publisher's predicate-to-wait transition.
                std::lock_guard<std::mutex> lock(owner.mutex_);
                owner.changed_.notify_all();
            }
        }
    };
    struct update_guard {
        registration_registry& owner;
        std::unique_lock<std::mutex>& lock;
        ~update_guard() {
            if (!lock.owns_lock()) lock.lock();
            if (!owner.reader_) owner.reader_.emplace(owner.writer_.borrow_ro());
            for (auto& s : owner.slots_) s.state.fetch_and(~closed, std::memory_order_release);
            owner.updating_ = false;
            lock.unlock();
            owner.changed_.notify_all();
        }
    };
public:
    enum class update_result { ok, reentrant, busy, timeout, rejected };
    // active is sampled across slots while readers may be entering and leaving.
    struct admission_state { bool updating; std::size_t active, waiting; };

    registration_registry() : writer_(lease::access::make_rw<lease::access::shared>(router_)),
                              reader_(writer_.borrow_ro()) {}
    registration_registry(const registration_registry&) = delete;
    admission_state state() {
        std::lock_guard<std::mutex> lock(mutex_);
        return {updating_, active_count(), waiting_};
    }
    wire::errc dispatch(const wire::bytes& input, wire::bytes& output, wire::limits limits = {}) {
        auto& s = slots_[thread_slot()];
        if (!enter(s)) {
            typed_rpc::envelope header;
            typed_rpc::read_envelope(wire::view(input), typed_rpc::request_magic, header, limits);
            return typed_rpc::error_reply(output, header, wire::errc::handler_failed, limits);
        }
        active_call call{*this, s};
        scope executing(this);
        return reader_->read([&](const router& r) { return r.dispatch_into(input, output, limits); });
    }

    // Registration callbacks may append methods only and must not retain the router.
    template<class Register>
    update_result publish(std::shared_ptr<void> module, Register&& registration,
                          std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
        if (scope::contains(this)) return update_result::reentrant;
        std::unique_lock<std::mutex> lock(mutex_);
        if (updating_) return update_result::busy;
        updating_ = true;
        for (auto& s : slots_) s.state.fetch_or(closed, std::memory_order_acq_rel);
        update_guard updating{*this, lock};
        if (!changed_.wait_for(lock, timeout, [&] { return active_count() == 0; }))
            return update_result::timeout;
        // One shared read lease covers the entire interval between publications.
        // Only a drained publisher can destroy it and obtain exclusive access.
        reader_.reset();
        lock.unlock();
        scope executing(this);
        auto status = writer_.write([&](router& r) {
            const auto before = r.size();
            if (module) pins_.push_back(module);
            // On exception retain the pin: earlier appends may already be live.
            const auto result = registration(r);
            if (module && r.size() == before) pins_.pop_back();
            return result;
        });
        return status == wire::errc::ok ? update_result::ok : update_result::rejected;
    }
};
}}
#endif
