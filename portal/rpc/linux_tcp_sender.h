#ifndef PORTAL_RPC_LINUX_TCP_SENDER_H
#define PORTAL_RPC_LINUX_TCP_SENDER_H

#include "detail/tcp_send.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <linux/errqueue.h>
#include <array>
#include <cerrno>
#include <system_error>

namespace portal { namespace rpc {
namespace detail {
// Entries correspond to successful nonempty MSG_ZEROCOPY submissions. Copied
// bytes behind an outstanding entry retire together with that entry.
class tcp_send_window {
    struct entry { std::size_t bytes; bool done; };
    std::array<entry, 64> entries_{};
    std::uint32_t head_, tail_;
    std::size_t bytes_ = 0;
    entry& at(std::uint32_t id) noexcept { return entries_[id % entries_.size()]; }
public:
    explicit tcp_send_window(std::uint32_t first = 0) noexcept : head_(first), tail_(first) {}
    bool full() const noexcept { return tail_ - head_ == entries_.size(); }
    std::size_t bytes() const noexcept { return bytes_; }
    void submit(std::size_t size) noexcept {
        at(tail_++) = {size, false};
        bytes_ += size;
    }
    std::size_t copied(std::size_t size) noexcept {
        if (head_ == tail_) return size;
        at(tail_ - 1).bytes += size;
        bytes_ += size;
        return 0;
    }
    bool complete(std::uint32_t first, std::uint32_t last, std::size_t& released) noexcept {
        if (first - head_ > last - head_ || last - head_ >= tail_ - head_) return false;
        for (auto id = first;; ++id) {
            if (at(id).done) return false;
            if (id == last) break;
        }
        for (auto id = first;; ++id) {
            at(id).done = true;
            if (id == last) break;
        }
        while (head_ != tail_ && at(head_).done) {
            const auto size = at(head_++).bytes;
            bytes_ -= size;
            released += size;
        }
        return true;
    }
};
}

// One consumer exclusively owns sending and the error queue of a fresh IPv4
// TCP socket. The caller owns the fd and storage, and must retain submitted
// spans until their prefix is released, including after socket errors.
class linux_tcp_sender {
public:
    using progress = detail::tcp_send_progress;
    using statistics = detail::tcp_send_statistics;
    static constexpr std::size_t default_threshold() noexcept { return 65535; }
    // A zero threshold selects ordinary sendmsg for the life of the sender.
    explicit linux_tcp_sender(int fd, std::size_t threshold = default_threshold()) noexcept
        : fd_(fd), threshold_(threshold) {
#if defined(SO_ZEROCOPY) && defined(MSG_ZEROCOPY) && defined(SO_EE_ORIGIN_ZEROCOPY) && defined(SO_EE_CODE_ZEROCOPY_COPIED)
        const int one = 1;
        enabled_ = threshold && ::setsockopt(fd, SOL_SOCKET, SO_ZEROCOPY, &one, sizeof(one)) == 0;
#endif
    }
    linux_tcp_sender(const linux_tcp_sender&) = delete;
    linux_tcp_sender& operator=(const linux_tcp_sender&) = delete;

    bool zerocopy_enabled() const noexcept { return enabled_; }
    bool window_full() const noexcept { return window_.full(); }
    std::size_t outstanding_bytes() const noexcept { return window_.bytes(); }
    const statistics& stats() const noexcept { return stats_; }

    progress send(wire::view first, wire::view second = {}) noexcept {
        progress result;
        if (!detail::valid_tcp_spans(first, second, window_.bytes())) {
            result.error = std::make_error_code(std::errc::invalid_argument);
            return result;
        }
        const auto size = first.size + second.size;
        if (!size) return result;
        int flags = MSG_NOSIGNAL;
        bool use_zc = enabled_ && size >= threshold_;
#ifdef MSG_ZEROCOPY
        if (use_zc) {
            if (window_.full()) {
                result.error = std::make_error_code(std::errc::resource_unavailable_try_again);
                return result;
            }
            flags |= MSG_ZEROCOPY;
        }
#endif
        result = detail::send_tcp_spans(fd_, first, second, flags);
        if (use_zc && result.error == std::errc::no_buffer_space) {
            ++stats_.resource_fallbacks;
            use_zc = false;
            result = detail::send_tcp_spans(fd_, first, second, MSG_NOSIGNAL);
        }
        if (!result.error) {
            if (use_zc) {
                window_.submit(result.accepted);
                ++stats_.zero_copy_sends;
            } else result.released = window_.copied(result.accepted);
        }
        return result;
    }

    // Nonblocking. Apply released even when a later notification reports an
    // error. A completed ID may be behind an earlier, still-pinned prefix.
    progress reap() noexcept {
        progress result;
#if defined(SO_ZEROCOPY) && defined(MSG_ZEROCOPY) && defined(SO_EE_ORIGIN_ZEROCOPY) && defined(SO_EE_CODE_ZEROCOPY_COPIED)
        while (window_.bytes()) {
            alignas(cmsghdr) unsigned char control[256];
            msghdr msg{};
            msg.msg_control = control;
            msg.msg_controllen = sizeof(control);
            if (::recvmsg(fd_, &msg, MSG_ERRQUEUE | MSG_DONTWAIT) < 0) {
                if (errno == EINTR) continue;
                if (errno != EAGAIN && errno != EWOULDBLOCK)
                    result.error = std::error_code(errno, std::generic_category());
                break;
            }
            if (msg.msg_flags & MSG_CTRUNC) {
                result.error = std::make_error_code(std::errc::message_size);
                break;
            }
            for (auto* c = CMSG_FIRSTHDR(&msg); c; c = CMSG_NXTHDR(&msg, c)) {
                if (c->cmsg_level != SOL_IP || c->cmsg_type != IP_RECVERR) continue;
                if (c->cmsg_len < CMSG_LEN(sizeof(sock_extended_err))) {
                    result.error = std::make_error_code(std::errc::protocol_error);
                    return result;
                }
                sock_extended_err e{};
                std::memcpy(&e, CMSG_DATA(c), sizeof(e));
                if (e.ee_errno) {
                    result.error = std::error_code(e.ee_errno, std::generic_category());
                    return result;
                }
                if (e.ee_origin != SO_EE_ORIGIN_ZEROCOPY) continue;
                if (!window_.complete(e.ee_info, e.ee_data, result.released)) {
                    result.error = std::make_error_code(std::errc::protocol_error);
                    return result;
                }
                if (e.ee_code & SO_EE_CODE_ZEROCOPY_COPIED)
                    stats_.kernel_copied += std::uint32_t(e.ee_data - e.ee_info + 1);
            }
        }
#endif
        return result;
    }
private:
    int fd_;
    std::size_t threshold_;
    bool enabled_ = false;
    detail::tcp_send_window window_{};
    statistics stats_{};
};
}}
#endif
