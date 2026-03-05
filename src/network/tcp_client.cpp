#include "network/tcp_client.h"
#include <spdlog/spdlog.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

namespace raftkv {

TcpClient::TcpClient(const std::string& host, uint16_t port,
                     MessageHandler on_message)
    : host_(host), port_(port), on_message_(std::move(on_message)) {}

TcpClient::~TcpClient() { disconnect(); }

bool TcpClient::connect() {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        spdlog::error("Failed to create socket: {}", strerror(errno));
        return false;
    }

    // Resolve hostname
    struct addrinfo hints{}, *result;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string port_str = std::to_string(port_);

    if (getaddrinfo(host_.c_str(), port_str.c_str(), &hints, &result) != 0) {
        spdlog::error("Failed to resolve {}: {}", host_, strerror(errno));
        close(fd_);
        fd_ = -1;
        return false;
    }

    if (::connect(fd_, result->ai_addr, result->ai_addrlen) < 0) {
        spdlog::error("Failed to connect to {}:{}: {}", host_, port_,
                      strerror(errno));
        freeaddrinfo(result);
        close(fd_);
        fd_ = -1;
        return false;
    }

    freeaddrinfo(result);
    connected_ = true;
    recv_thread_ = std::thread(&TcpClient::recv_loop, this);
    spdlog::info("Connected to {}:{}", host_, port_);
    return true;
}

void TcpClient::disconnect() {
    connected_ = false;
    if (fd_ >= 0) {
        shutdown(fd_, SHUT_RDWR);
        close(fd_);
        fd_ = -1;
    }
    if (recv_thread_.joinable()) recv_thread_.join();
}

bool TcpClient::send(const RaftMessage& msg) {
    if (!connected_) return false;
    std::lock_guard lock(send_mutex_);
    auto buf = MessageFramer::frame(msg);
    ssize_t sent = ::send(fd_, buf.data(), buf.size(), 0);
    return sent == static_cast<ssize_t>(buf.size());
}

void TcpClient::recv_loop() {
    MessageFramer framer;
    uint8_t buf[4096];

    while (connected_) {
        ssize_t n = recv(fd_, buf, sizeof(buf), 0);
        if (n <= 0) {
            if (connected_ && n < 0)
                spdlog::warn("recv error: {}", strerror(errno));
            connected_ = false;
            break;
        }

        framer.feed(buf, static_cast<size_t>(n));
        while (auto msg = framer.poll()) {
            on_message_(*msg);
        }
    }
}

}  // namespace raftkv
