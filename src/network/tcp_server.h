#pragma once

#include "network/message.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace raftkv {

// Callback invoked when a complete message arrives from a connected peer.
// Parameters: sender socket fd, the parsed RaftMessage.
using MessageHandler = std::function<void(int fd, const RaftMessage& msg)>;

// Callback invoked when a peer connection is closed.
using DisconnectHandler = std::function<void(int fd)>;

class TcpServer {
public:
    TcpServer(uint16_t port, MessageHandler on_message,
              DisconnectHandler on_disconnect = nullptr);
    ~TcpServer();

    void start();
    void stop();

    // Send a message to a specific connected client fd
    bool send(int fd, const RaftMessage& msg);

private:
    void accept_loop();
    void client_loop(int fd);

    uint16_t port_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};

    MessageHandler on_message_;
    DisconnectHandler on_disconnect_;

    std::thread accept_thread_;
    std::mutex clients_mutex_;
    std::unordered_map<int, std::thread> client_threads_;
};

}  // namespace raftkv
