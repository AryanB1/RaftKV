#include "network/tcp_server.h"
#include <spdlog/spdlog.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace raftkv {

TcpServer::TcpServer(uint16_t port, MessageHandler on_message,
                     DisconnectHandler on_disconnect)
    : port_(port), on_message_(std::move(on_message)),
      on_disconnect_(std::move(on_disconnect)) {}

TcpServer::~TcpServer() { stop(); }

void TcpServer::start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        spdlog::error("Failed to create socket: {}", strerror(errno));
        return;
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        spdlog::error("Failed to bind on port {}: {}", port_, strerror(errno));
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    if (listen(listen_fd_, 16) < 0) {
        spdlog::error("Failed to listen: {}", strerror(errno));
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    running_ = true;
    accept_thread_ = std::thread(&TcpServer::accept_loop, this);
    spdlog::info("TCP server listening on port {}", port_);
}

void TcpServer::stop() {
    running_ = false;
    if (listen_fd_ >= 0) {
        shutdown(listen_fd_, SHUT_RDWR);
        close(listen_fd_);
        listen_fd_ = -1;
    }
    if (accept_thread_.joinable()) accept_thread_.join();

    std::lock_guard lock(clients_mutex_);
    for (auto& [fd, thr] : client_threads_) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
        if (thr.joinable()) thr.join();
    }
    client_threads_.clear();
}

bool TcpServer::send(int fd, const RaftMessage& msg) {
    auto buf = MessageFramer::frame(msg);
    ssize_t sent = ::send(fd, buf.data(), buf.size(), 0);
    return sent == static_cast<ssize_t>(buf.size());
}

void TcpServer::accept_loop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = accept(listen_fd_,
                               reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client_fd < 0) {
            if (running_) spdlog::warn("Accept failed: {}", strerror(errno));
            continue;
        }

        spdlog::info("Accepted connection from {}:{} (fd={})",
                     inet_ntoa(client_addr.sin_addr),
                     ntohs(client_addr.sin_port), client_fd);

        std::lock_guard lock(clients_mutex_);
        client_threads_[client_fd] =
            std::thread(&TcpServer::client_loop, this, client_fd);
    }
}

void TcpServer::client_loop(int fd) {
    MessageFramer framer;
    uint8_t buf[4096];

    while (running_) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) {
            if (n < 0 && errno != ECONNRESET)
                spdlog::warn("recv error on fd {}: {}", fd, strerror(errno));
            break;
        }

        framer.feed(buf, static_cast<size_t>(n));
        while (auto msg = framer.poll()) {
            on_message_(fd, *msg);
        }
    }

    close(fd);
    if (on_disconnect_) on_disconnect_(fd);
    spdlog::info("Connection closed (fd={})", fd);
}

}  // namespace raftkv
