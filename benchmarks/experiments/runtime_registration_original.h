#pragma once
#include <portal/rpc/typed.h>
#include <lease.hpp>
#include <condition_variable>
#include <mutex>
#include <chrono>
#include <vector>
#include <algorithm>
#include <functional>
#include <memory>
// Frozen control from the 2026-11-16 registration prototype.
namespace registration_prototype {
namespace rpc = portal::typed_rpc;
namespace wire = portal::wire;
// Application-owned gate. No raw router or lease ever escapes this object.
class coordinator {
    using RO = lease::access::shared_access<rpc::router, lease::access::shared>;
    using RW = lease::access::exclusive_access<rpc::router, lease::access::shared>;
    // reverse destruction: writer, router (including JIT destructors), then modules.
    std::vector<std::shared_ptr<void>> module_pins_;
    rpc::router router_;
    RW writer_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool updating_ = false;
    std::size_t active_ = 0, queued_ = 0;
    static std::vector<const coordinator*>& stack() {
        static thread_local std::vector<const coordinator*> value;
        return value;
    }
    bool inside() const {
        const auto& s = stack(); return std::find(s.begin(),s.end(),this)!=s.end();
    }
    struct loan {
        coordinator& owner;
        std::unique_ptr<RO> reader;
        ~loan() {
            reader.reset(); // MUST precede externally visible active == 0
            std::lock_guard<std::mutex> lock(owner.mutex_);
            --owner.active_; owner.cv_.notify_all();
        }
    };
    struct scope {
        scope(const coordinator* c) { stack().push_back(c); }
        ~scope() { stack().pop_back(); }
    };
public:
    enum class update_result { ok, reentrant, busy, timeout, rejected };
    coordinator() : writer_(lease::access::make_rw<lease::access::shared>(router_)) {}
    coordinator(const coordinator&) = delete;
    // Destruction is only legal after owner has stopped/joined all callers.
    wire::errc dispatch(const wire::bytes& input, wire::bytes& output) {
        std::unique_lock<std::mutex> lock(mutex_);
        // A nested request cannot wait behind a writer waiting on its outer request.
        if (updating_ && inside()) return wire::errc::handler_failed;
        ++queued_; cv_.notify_all();
        cv_.wait(lock,[&]{return !updating_;});
        --queued_;
        auto reader = std::unique_ptr<RO>(new RO(writer_.borrow_ro()));
        ++active_;
        loan held{*this,std::move(reader)};
        lock.unlock();
        scope current(this);
        return held.reader->read([&](const rpc::router& r) {
            return r.dispatch_into(input, output);
        });
    }
    update_result publish(std::shared_ptr<void> module,
                          const std::function<wire::errc(rpc::router&)>& registration,
                          std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
        if (inside()) return update_result::reentrant;
        std::unique_lock<std::mutex> lock(mutex_);
        if (updating_) return update_result::busy;
        updating_ = true; cv_.notify_all();
        if (!cv_.wait_for(lock,timeout,[&]{return active_==0;})) {
            updating_=false; cv_.notify_all(); return update_result::timeout;
        }
        // Pin BEFORE defining: allocation failure cannot leave an unpinned entry.
        // Failed/duplicate registration pins are retained too, until destruction.
        try {
            module_pins_.push_back(std::move(module));
            // Do not hold gate mutex over user registration code. updating_ keeps
            // readers out and other writers return busy. Mark reentry explicitly.
            lock.unlock();
            scope current(this);
            auto result = writer_.write([&](rpc::router& r){ return registration(r); });
            lock.lock(); updating_=false; cv_.notify_all();
            return result==wire::errc::ok ? update_result::ok : update_result::rejected;
        } catch (...) {
            if (!lock.owns_lock()) lock.lock();
            updating_=false; cv_.notify_all(); throw;
        }
    }
    // Test observation only. Never used to decide whether lease may be written.
    bool wait_state(bool updating, std::size_t min_queued,
                    std::chrono::milliseconds timeout=std::chrono::seconds(2)) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock,timeout,[&]{return updating_==updating && queued_>=min_queued;});
    }
};
}
