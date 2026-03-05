#include "network/tcp_client.h"
#include <spdlog/spdlog.h>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

struct NodeAddr {
    std::string host;
    uint16_t port;
};

// Synchronous client wrapper: sends a request and blocks until a response arrives.
class KvClient {
public:
    KvClient(const std::vector<NodeAddr>& nodes) : nodes_(nodes) {}

    bool connect_to(size_t index) {
        if (index >= nodes_.size()) return false;
        disconnect();

        current_node_ = index;
        client_ = std::make_unique<raftkv::TcpClient>(
            nodes_[index].host, nodes_[index].port,
            [this](const raftkv::RaftMessage& msg) {
                std::lock_guard lock(resp_mu_);
                last_response_ = msg;
                got_response_ = true;
                resp_cv_.notify_one();
            });

        if (!client_->connect()) {
            client_.reset();
            return false;
        }
        return true;
    }

    void disconnect() {
        if (client_) {
            client_->disconnect();
            client_.reset();
        }
    }

    bool is_connected() const { return client_ && client_->is_connected(); }

    // Connect to any available node
    bool connect_any() {
        for (size_t i = 0; i < nodes_.size(); i++) {
            if (connect_to(i)) return true;
        }
        return false;
    }

    // Send a command and wait for response. Handles leader redirects automatically.
    // Returns {success, output_text}
    std::pair<bool, std::string> execute(const std::string& command, int max_redirects = 3) {
        for (int attempt = 0; attempt <= max_redirects; attempt++) {
            if (!is_connected()) {
                if (!connect_any()) {
                    return {false, "ERR: cannot connect to any node"};
                }
            }

            // Send request
            raftkv::RaftMessage msg;
            msg.set_type(raftkv::RaftMessage::CLIENT_REQ);
            msg.set_sender_id(0);
            auto* req = msg.mutable_client_req();
            req->set_command(command);

            {
                std::lock_guard lock(resp_mu_);
                got_response_ = false;
            }

            if (!client_->send(msg)) {
                disconnect();
                continue;
            }

            // Wait for response with timeout
            std::unique_lock lock(resp_mu_);
            bool received = resp_cv_.wait_for(lock, std::chrono::seconds(5),
                                               [this] { return got_response_; });

            if (!received) {
                disconnect();
                return {false, "ERR: request timed out"};
            }

            auto& resp = last_response_;
            if (!resp.has_client_resp()) {
                return {false, "ERR: unexpected response type"};
            }

            auto& cr = resp.client_resp();
            if (cr.success()) {
                return {true, cr.value()};
            }

            // Not leader — follow redirect
            if (cr.error() == "Not leader" && cr.leader_hint() != 0) {
                uint32_t leader_hint = cr.leader_hint();
                // Find the node with this id — node ids are 1-based, map to 0-based index
                size_t target = leader_hint - 1;
                if (target < nodes_.size()) {
                    spdlog::info("Redirecting to leader (node {}) at {}:{}",
                                 leader_hint, nodes_[target].host, nodes_[target].port);
                    if (connect_to(target)) continue;
                }
                // Couldn't connect to hinted leader, try all
                disconnect();
                continue;
            }

            return {false, "ERR: " + cr.error()};
        }

        return {false, "ERR: too many redirects"};
    }

private:
    std::vector<NodeAddr> nodes_;
    size_t current_node_ = 0;
    std::unique_ptr<raftkv::TcpClient> client_;

    std::mutex resp_mu_;
    std::condition_variable resp_cv_;
    bool got_response_ = false;
    raftkv::RaftMessage last_response_;
};

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " <host:port> [host:port ...]\n"
              << "  Provide one or more node addresses.\n"
              << "  The client will auto-discover the leader.\n"
              << "\n"
              << "Commands:\n"
              << "  PUT <key> <value>  - Store a key-value pair\n"
              << "  GET <key>          - Retrieve a value by key\n"
              << "  DELETE <key>       - Remove a key\n"
              << "  LIST               - List all keys\n"
              << "  quit / exit        - Exit the client\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    spdlog::set_pattern("[%H:%M:%S.%e] [%^%l%$] %v");
    spdlog::set_level(spdlog::level::warn);

    // Parse node addresses
    std::vector<NodeAddr> nodes;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        auto colon = arg.rfind(':');
        if (colon == std::string::npos) {
            std::cerr << "Invalid address: " << arg << " (expected host:port)\n";
            return 1;
        }
        nodes.push_back({arg.substr(0, colon),
                         static_cast<uint16_t>(std::stoul(arg.substr(colon + 1)))});
    }

    KvClient client(nodes);

    if (!client.connect_any()) {
        std::cerr << "Failed to connect to any node.\n";
        return 1;
    }

    std::string line;
    std::cout << "raftkv> " << std::flush;
    while (std::getline(std::cin, line)) {
        if (line.empty()) {
            std::cout << "raftkv> " << std::flush;
            continue;
        }

        // Trim whitespace
        auto start = line.find_first_not_of(" \t");
        if (start == std::string::npos) {
            std::cout << "raftkv> " << std::flush;
            continue;
        }
        line = line.substr(start);

        if (line == "quit" || line == "exit") break;

        if (line == "help") {
            print_usage("raftkv_client");
            std::cout << "raftkv> " << std::flush;
            continue;
        }

        auto [success, output] = client.execute(line);
        if (!output.empty()) {
            std::cout << output << std::endl;
        }

        std::cout << "raftkv> " << std::flush;
    }

    std::cout << "Bye!" << std::endl;
    return 0;
}
