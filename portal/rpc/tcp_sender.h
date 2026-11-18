#ifndef PORTAL_RPC_TCP_SENDER_H
#define PORTAL_RPC_TCP_SENDER_H

#if defined(__linux__)
#include "linux_tcp_sender.h"

namespace portal { namespace rpc {
using tcp_sender = linux_tcp_sender;
}}
#elif defined(__APPLE__)
#include "detail/tcp_send.h"

namespace portal { namespace rpc {
class tcp_sender {
public:
    using progress = detail::tcp_send_progress;
    using statistics = detail::tcp_send_statistics;
    static constexpr std::size_t default_threshold() noexcept { return 0; }

    // The socket remains caller-owned. SO_NOSIGPIPE is a per-socket option.
    explicit tcp_sender(int fd, std::size_t = default_threshold()) noexcept : fd_(fd) {
        const int one = 1;
        if (::setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)))
            setup_error_ = std::error_code(errno, std::generic_category());
    }
    tcp_sender(const tcp_sender&) = delete;
    tcp_sender& operator=(const tcp_sender&) = delete;

    bool zerocopy_enabled() const noexcept { return false; }
    bool window_full() const noexcept { return false; }
    std::size_t outstanding_bytes() const noexcept { return 0; }
    const statistics& stats() const noexcept {
        static constexpr statistics counters{};
        return counters;
    }
    progress reap() noexcept { return {}; }

    progress send(wire::view first, wire::view second = {}) noexcept {
        if (!detail::valid_tcp_spans(first, second))
            return {std::make_error_code(std::errc::invalid_argument), 0, 0};
        if (!first.size && !second.size) return {};
        if (setup_error_) return {setup_error_, 0, 0};
        auto result = detail::send_tcp_spans(fd_, first, second, 0);
        result.released = result.accepted;
        return result;
    }
private:
    int fd_;
    std::error_code setup_error_;
};
}}
#else
#error "portal/rpc/tcp_sender.h requires Linux or macOS"
#endif
#endif
