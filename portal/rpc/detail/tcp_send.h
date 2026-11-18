#ifndef PORTAL_RPC_DETAIL_TCP_SEND_H
#define PORTAL_RPC_DETAIL_TCP_SEND_H

#include "../../wire/schema.h"
#include <cerrno>
#include <system_error>
#include <sys/socket.h>
#include <sys/uio.h>

namespace portal { namespace rpc { namespace detail {
struct tcp_send_progress {
    std::error_code error{};
    std::size_t accepted = 0, released = 0;
};
struct tcp_send_statistics {
    std::uint64_t zero_copy_sends = 0, kernel_copied = 0, resource_fallbacks = 0;
};

inline bool valid_tcp_spans(wire::view first, wire::view second, std::size_t outstanding = 0) noexcept {
    const auto maximum = static_cast<std::size_t>((std::numeric_limits<ssize_t>::max)());
    return first.size <= maximum && second.size <= maximum - first.size
        && first.size + second.size <= (std::numeric_limits<std::size_t>::max)() - outstanding
        && (!first.size || first.data) && (!second.size || second.data);
}

// Called with validated nonempty input. A short send accepts a prefix across both spans.
inline tcp_send_progress send_tcp_spans(int fd, wire::view first, wire::view second, int flags) noexcept {
    iovec parts[] = {{const_cast<std::uint8_t*>(first.data), first.size},
                     {const_cast<std::uint8_t*>(second.data), second.size}};
    msghdr msg{};
    msg.msg_iov = parts;
    msg.msg_iovlen = second.size ? 2 : 1;
    ssize_t count;
    do { count = ::sendmsg(fd, &msg, flags); } while (count < 0 && errno == EINTR);
    if (count <= 0) return {std::error_code(count ? errno : EIO, std::generic_category()), 0, 0};
    return {{}, static_cast<std::size_t>(count), 0};
}
}}}
#endif
