#include "fixture.h"
#include "portal/rpc/tcp_sender.h"
#include <arpa/inet.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>

using portal::rpc::tcp_sender;

static void mixed_sends() {
    const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK(listener >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(addr);
    CHECK(!::bind(listener, reinterpret_cast<sockaddr*>(&addr), length));
    CHECK(!::listen(listener, 1));
    CHECK(!::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &length));
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0 && !::connect(fd, reinterpret_cast<sockaddr*>(&addr), length));
    const int peer = ::accept(listener, nullptr, nullptr);
    CHECK(peer >= 0);
    ::close(listener);
    tcp_sender sender(fd);
    w::bytes expected(65537, 'a'), received(expected.size());
    expected.front() = 0;
    expected.back() = 255;
    const auto first = sender.send({expected.data(), 1});
    CHECK(!first.error && first.accepted == 1 && first.released == 1);
    const auto large = sender.send({expected.data() + 1, 65530}, {expected.data() + 65531, 5});
    CHECK(!large.error && large.accepted == 65535);
    if (sender.zerocopy_enabled())
        CHECK(sender.stats().zero_copy_sends + sender.stats().resource_fallbacks == 1);
    const bool pinned = sender.stats().zero_copy_sends == 1;
    CHECK(large.released == (pinned ? 0u : 65535u));
    const auto last = sender.send({&expected.back(), 1});
    CHECK(!last.error && last.accepted == 1 && last.released == (pinned ? 0u : 1u));
    CHECK(sender.outstanding_bytes() == (pinned ? 65536u : 0u));
    CHECK(::recv(peer, received.data(), received.size(), MSG_WAITALL) == ssize_t(received.size()));
    CHECK(received == expected);
    std::size_t released = first.released + large.released + last.released;
    while (sender.outstanding_bytes()) {
        pollfd p{fd, 0, 0};
        CHECK(::poll(&p, 1, 5000) == 1 && (p.revents & POLLERR));
        const auto done = sender.reap();
        CHECK(!done.error);
        released += done.released;
    }
    CHECK(released == expected.size());
    ::close(fd);
    ::close(peer);
}

static void unavailable_and_short_write() {
    int pair[2];
    CHECK(!::socketpair(AF_UNIX, SOCK_STREAM, 0, pair));
    CHECK(!::fcntl(pair[0], F_SETFL, O_NONBLOCK));
    tcp_sender sender(pair[0]);
    CHECK(!sender.zerocopy_enabled());
    const int limit = 4096;
    CHECK(!::setsockopt(pair[0], SOL_SOCKET, SO_SNDBUF, &limit, sizeof(limit)));
    w::bytes payload(262144, 123);
    const auto sent = sender.send(w::view(payload));
    CHECK(!sent.error && sent.accepted > 0 && sent.accepted < payload.size());
    CHECK(sent.accepted == sent.released && !sender.outstanding_bytes());
    const auto blocked = sender.send(w::view(payload));
    CHECK(blocked.error == std::errc::operation_would_block && !blocked.accepted && !blocked.released);
    CHECK(!sender.stats().zero_copy_sends);
    ::close(pair[1]);
    const auto closed = sender.send(w::view(payload));
    CHECK(closed.error && !closed.accepted && !closed.released);
    ::close(pair[0]);
}

int main() {
    mixed_sends();
    unavailable_and_short_write();
}
