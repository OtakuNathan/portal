#ifndef PORTAL_BENCHMARKS_RPC_RING_H
#define PORTAL_BENCHMARKS_RPC_RING_H

#include "portal/portal.h"
#include "portal/backends/rpc.h"
#include "portal/wire/schema.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <system_error>
#include <utility>

namespace rpc_ring_bench {
namespace codec = portal::rpc::detail;
using portal::rpc::bytes;
using portal::rpc::value;

inline void check(bool ok) {
    if (!ok) {
        std::fputs("ring benchmark validation failed\n", stderr);
        std::abort();
    }
}

// A producer may fill only its unpublished reservation. At most two physical
// spans are needed, even when a field crosses the end of the ring.
class ring_sink {
    bytes& storage_;
    std::size_t offset_;
    std::size_t remaining_;
public:
    ring_sink(bytes& storage, std::size_t offset, std::size_t size)
        : storage_(storage), offset_(offset), remaining_(size) {}

    void append(const void* data, std::size_t size) {
        check(size <= remaining_);
        if (!size) return;
        const auto first = std::min(size, storage_.size() - offset_);
        std::memcpy(storage_.data() + offset_, data, first);
        if (first < size) {
            std::memcpy(storage_.data(), static_cast<const std::uint8_t*>(data) + first, size - first);
        }
        offset_ = (offset_ + size) & (storage_.size() - 1);
        remaining_ -= size;
    }
    void put_byte(std::uint8_t byte) { append(&byte, 1); }
    bool complete() const { return remaining_ == 0; }
};

enum class write_status { published, full, oversized, invalid };

// A waiter lives until its single completion. Stop pending waits and drain
// their continuations before destroying the ring.
struct space_waiter {
    virtual void complete(std::error_code error) noexcept = 0;
protected:
    ~space_waiter() = default;
};

class byte_ring {
    bytes storage_;
    const std::size_t high_water_;
    // One producer can wait at a time. While it sleeps, tail stays fixed.
    static constexpr std::size_t inactive = std::numeric_limits<std::size_t>::max();
    std::mutex wait_mutex_;
    std::condition_variable space_;
    std::atomic<std::size_t> resume_at_{inactive};
    space_waiter* pending_ = nullptr; // Protected by wait_mutex_.
    bool waits_stopped_ = false;

    std::size_t max_queued_before(std::size_t frame_size) const {
        // A frame above the watermark gets sole use of an empty ring.
        return frame_size < high_water_ ? high_water_ - frame_size : 0;
    }

    std::size_t resume_limit(std::size_t frame_size) const {
        return std::min(high_water_ / 2, max_queued_before(frame_size));
    }

    space_waiter* take_pending() {
        resume_at_.store(inactive);
        return std::exchange(pending_, nullptr);
    }

    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
    template<class Fill>
    write_status try_fill(std::size_t size, Fill fill) {
        const auto capacity = storage_.size();
        if (size > capacity) return write_status::oversized;
        const auto tail = tail_.load(std::memory_order_relaxed);
        const auto used = tail - head_.load(std::memory_order_acquire);
        if (used > max_queued_before(size)) return write_status::full;
        if (!fill(tail & (capacity - 1))) return write_status::invalid;
        tail_.store(tail + size, std::memory_order_release);
        return write_status::published;
    }

public:
    byte_ring(std::size_t capacity, std::size_t high_water)
        : storage_(capacity), high_water_(high_water) {
        check(capacity && (capacity & (capacity - 1)) == 0);
        check(high_water && high_water <= capacity);
    }

    template<class Fill>
    write_status try_write(std::size_t size, Fill fill) {
        return try_fill(size, [&](std::size_t offset) {
            codec::field_writer<ring_sink> out(storage_, offset, size);
            fill(out);
            check(out.complete());
            return true;
        });
    }

    template<class Request>
    write_status try_write_typed(const Request& request) {
        if (request.error() != portal::wire::errc::ok) return write_status::invalid;
        const auto size = request.size();
        return try_fill(size, [&](std::size_t offset) {
            const auto first = std::min(size, storage_.size() - offset);
            portal::wire::writer out(storage_.data() + offset, first, storage_.data(), size - first);
            return request.write(out) == portal::wire::errc::ok;
        });
    }

    bool producer_paused() const {
        return resume_at_.load() != inactive;
    }

    // Entered after a full write. Arm first and recheck head to cover a
    // concurrent drain between the failed write and waiter registration.
    void wait_async(std::size_t size, space_waiter& waiter) noexcept {
        std::error_code error;
        {
            std::lock_guard<std::mutex> lock(wait_mutex_);
            if (waits_stopped_) error = std::make_error_code(std::errc::operation_canceled);
            else if (size > storage_.size()) error = std::make_error_code(std::errc::message_size);
            else if (producer_paused()) error = std::make_error_code(std::errc::operation_in_progress);
            else {
                const auto limit = resume_limit(size);
                resume_at_.store(limit);
                if (tail_.load(std::memory_order_relaxed) - head_.load() > limit) {
                    pending_ = &waiter;
                    return;
                }
                resume_at_.store(inactive);
            }
        }
        waiter.complete(error);
    }

    void stop_async_waits() noexcept {
        space_waiter* waiter = nullptr;
        {
            std::lock_guard<std::mutex> lock(wait_mutex_);
            waits_stopped_ = true;
            if (pending_) waiter = take_pending();
        }
        if (waiter) waiter->complete(std::make_error_code(std::errc::operation_canceled));
    }

    template<class Fill>
    write_status write(std::size_t size, Fill fill) {
        const auto status = try_write(size, fill);
        if (status != write_status::full) return status;

        const auto tail = tail_.load(std::memory_order_relaxed);
        const auto limit = resume_limit(size);
        {
            std::unique_lock<std::mutex> lock(wait_mutex_);
            // Pair the SC store/load here with the consumer's head/store and
            // resume_at/load: either we see its progress or it sees our wait.
            resume_at_.store(limit);
            space_.wait(lock, [&] { return tail - head_.load() <= limit; });
            resume_at_.store(inactive);
        }
        // Only this producer can increase occupancy.
        return try_write(size, fill);
    }

    // A single consumer can submit bytes past an in-flight prefix. The spans
    // stay valid until release() advances the reclaim position over them.
    struct readable_spans {
        portal::wire::view first, second;
    };
    readable_spans peek(std::size_t skip, std::size_t maximum) const {
        const auto head = head_.load(std::memory_order_relaxed);
        const auto available = tail_.load(std::memory_order_acquire) - head;
        check(skip <= available);
        const auto size = std::min(maximum, available - skip);
        const auto offset = (head + skip) & (storage_.size() - 1);
        const auto first = std::min(size, storage_.size() - offset);
        return {{storage_.data() + offset, first}, {storage_.data(), size - first}};
    }

    void release(std::size_t used) {
        const auto head = head_.load(std::memory_order_relaxed);
        const auto available = tail_.load(std::memory_order_acquire) - head;
        check(used <= available);
        reclaim(head, available, used);
    }

    // The callback can accept any prefix, including zero. It must finish
    // reading the accepted bytes before it returns their count.
    template<class Consume>
    std::size_t read(Consume consume) {
        const auto head = head_.load(std::memory_order_relaxed);
        const auto available = tail_.load(std::memory_order_acquire) - head;
        if (!available) return 0;
        const auto offset = head & (storage_.size() - 1);
        const auto span = std::min(available, storage_.size() - offset);
        const std::size_t used = consume(storage_.data() + offset, span);
        check(used <= span);
        reclaim(head, available, used);
        return used;
    }
private:
    void reclaim(std::size_t head, std::size_t available, std::size_t used) {
        if (used) {
            head_.store(head + used);
            const auto limit = resume_at_.load();
            if (limit != inactive && available - used <= limit) {
                // Synchronize with the predicate-to-sleep transition. A stale
                // tail snapshot can signal early; the waiter rechecks occupancy.
                space_waiter* waiter = nullptr;
                {
                    std::lock_guard<std::mutex> lock(wait_mutex_);
                    if (pending_) {
                        const auto queued = tail_.load(std::memory_order_relaxed) - (head + used);
                        if (queued <= resume_at_.load()) waiter = take_pending();
                    } else {
                        space_.notify_one();
                    }
                }
                if (waiter) waiter->complete({});
            }
        }
    }
};
}

#endif
