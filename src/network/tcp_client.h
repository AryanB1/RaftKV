#pragma once

#include "network/message.h"
#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace raftkv {

// A TCP client that connects to a single remote node.
// Receives messages asynchronously via a callback.
class TcpClient {
public:
    using MessageHandler = std::function<void(const RaftMessage& msg)>;

    TcpClient(const std::string& host, uint16_t port, MessageHandler on_message);
    ~TcpClient();

    bool connect();
    void disconnect();
    bool is_connected() const { return connected_; }

    bool send(const RaftMessage& msg);

private:
    void recv_loop();

    std::string host_;
    uint16_t port_;
    int fd_ = -1;
    std::atomic<bool> connected_{false};
    std::mutex send_mutex_;

    MessageHandler on_message_;
    std::thread recv_thread_;
};

}  // namespace raftkv
