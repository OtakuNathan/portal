// IPv4 TCP send experiment. The peer drains the byte stream to EOF and
// replies with one byte. Each run owns a fresh socket and its error queue.
#include "rpc_ring.h"
#include "portal/rpc/typed_async.h"
#include "portal/rpc/tcp_sender.h"
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <ctime>

namespace {
using namespace rpc_ring_bench;
namespace w = portal::wire;
namespace tr = portal::typed_rpc;

[[noreturn]] void fail(const char* operation) {
    std::perror(operation);
    std::exit(EXIT_FAILURE);
}

struct sender {
    int fd;
    byte_ring& ring;
    portal::rpc::tcp_sender socket;

    sender(int fd, byte_ring& ring, std::size_t threshold)
        : fd(fd), ring(ring), socket(fd, threshold) {}

    bool send_batch() {
        if (socket.window_full()) return false;
        const auto spans = ring.peek(socket.outstanding_bytes(), 65535);
        if (!spans.first.size) return false;
        const auto sent = socket.send(spans.first, spans.second);
        if (sent.error) { errno = sent.error.value(); fail("sendmsg"); }
        if (sent.released) ring.release(sent.released);
        return true;
    }

    void await_completions() {
        pollfd p{fd, 0, 0}; // POLLERR is always reported.
        int ready;
        do { ready = ::poll(&p, 1, 5000); } while (ready < 0 && errno == EINTR);
        if (ready < 0) fail("poll");
        if (!ready) { errno = ETIMEDOUT; fail("zerocopy completion"); }
        check(p.revents & POLLERR);
        int socket_error = 0;
        socklen_t length = sizeof(socket_error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &length)) fail("SO_ERROR");
        if (socket_error) { errno = socket_error; fail("socket"); }
        const auto notified = socket.reap();
        if (notified.released) ring.release(notified.released);
        if (notified.error) { errno = notified.error.value(); fail("socket error queue"); }
    }
};

#ifdef __linux__
void check_reclaim() {
    byte_ring ring(16, 16);
    auto fill = [&](std::size_t n) {
        return ring.try_write(n, [n](auto& w) { for (std::size_t i = 0; i < n; ++i) w.put_byte(1); });
    };
    portal::rpc::detail::tcp_send_window f(UINT32_MAX - 1);
    check(fill(16) == write_status::published);
    f.submit(3); f.submit(5); f.submit(6);
    check(f.copied(2) == 0 && !ring.peek(f.bytes(), 16).first.size);
    struct waiter : space_waiter {
        unsigned calls = 0;
        void complete(std::error_code error) noexcept override { check(!error); ++calls; }
    } waiting;
    ring.wait_async(8, waiting);
    std::size_t n = 0;
    check(f.complete(UINT32_MAX, 0, n) && n == 0 && fill(1) == write_status::full);
    check(waiting.calls == 0 && ring.producer_paused());
    check(f.complete(UINT32_MAX - 1, UINT32_MAX - 1, n));
    check(n == 16 && !f.bytes());
    ring.release(n);
    check(waiting.calls == 1 && !ring.producer_paused());
    check(fill(16) == write_status::published);
}
#endif
}

int main(int argc, char** argv) {
#ifdef __linux__
    if (argc == 2 && !std::strcmp(argv[1], "--check")) { check_reclaim(); return 0; }
#endif
    const bool staged = argc == 4 && !std::strcmp(argv[1], "staged-copy");
    const bool force_zc = argc == 4 && !std::strcmp(argv[1], "zerocopy");
    const bool copy = argc == 4 && !std::strcmp(argv[1], "copy");
    if (argc != 3 && !staged && !force_zc && !copy) {
        std::fputs("usage: rpc_ring_tcp_benchmark [staged-copy|copy|zerocopy] IPv4 port\n", stderr);
        return 2;
    }
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) fail("socket");
    timeval timeout{5, 0};
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) ||
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout))) fail("socket timeout");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    char* end = nullptr;
    const long port = std::strtol(argv[argc - 1], &end, 10);
    check(!*end && port > 0 && port <= 65535);
    address.sin_port = htons(static_cast<std::uint16_t>(port));
    check(::inet_pton(AF_INET, argv[argc - 2], &address.sin_addr) == 1);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address))) fail("connect");

    using method = tr::method<1, 1, w::none, w::blob<65536>>;
    const tr::prepared_request<method> frames[] = {
        tr::prepared_request<method>({}, std::string(65499, 'a')),
        tr::prepared_request<method>({}, std::string(65499, 'b'))};
    byte_ring ring(1u << 20, 1u << 20);
    sender tx(fd, ring, argc == 3 ? portal::rpc::tcp_sender::default_threshold() : (force_zc ? 1u : 0u));
    if (force_zc && !tx.socket.zerocopy_enabled()) {
        std::fputs("SO_ZEROCOPY unavailable on this socket\n", stderr);
        return 1;
    }
    w::bytes scratch(staged ? frames[0].size() : 0);
    auto enqueue = [&](const tr::prepared_request<method>& frame) {
        if (!staged) return ring.try_write_typed(frame);
        return ring.try_write(frame.size(), [&](auto& out) {
            w::writer writer(scratch.data(), scratch.size());
            check(frame.write(writer) == w::errc::ok);
            out.append(scratch.data(), scratch.size());
        });
    };
    const auto start = std::chrono::steady_clock::now();
    const auto cpu = std::clock();
    constexpr unsigned count = 512;
    unsigned queued = 0;
    do {
        while (queued < count) {
            const auto status = enqueue(frames[queued % 2]);
            if (status == write_status::full) break;
            check(status == write_status::published);
            ++queued;
        }
        if (tx.send_batch()) continue;
        if (tx.socket.outstanding_bytes()) tx.await_completions();
    } while (queued < count || ring.peek(0, 1).first.size);
    if (::shutdown(fd, SHUT_WR)) fail("shutdown");
    char ack;
    check(::recv(fd, &ack, 1, MSG_WAITALL) == 1 && ack == 'K');
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    const double cpu_ms = 1000.0 * (std::clock() - cpu) / CLOCKS_PER_SEC;
    const auto stats = tx.socket.stats();
    std::printf("bytes=%zu Mbps=%.2f cpu_ms=%.3f zc_sends=%llu kernel_copied=%llu enobufs_fallback=%llu\n",
        frames[0].size() * count, frames[0].size() * count * 8e-6 / elapsed, cpu_ms,
        static_cast<unsigned long long>(stats.zero_copy_sends),
        static_cast<unsigned long long>(stats.kernel_copied),
        static_cast<unsigned long long>(stats.resource_fallbacks));
    ::close(fd);
}
