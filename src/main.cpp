#include "network/tcp_server.h"
#include "network/tcp_client.h"
#include "raft/raft_node.h"
#include <spdlog/spdlog.h>
#include <csignal>
#include <iostream>
#include <thread>
#include <vector>

static std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " <node_id> <port> <peer_id:host:port> [peer_id:host:port ...]\n"
              << "  node_id: unique integer ID for this node (1, 2, 3, ...)\n"
              << "  port: TCP port to listen on\n"
              << "  peers: list of peer_id:host:port\n"
              << "Example: " << prog << " 1 5001 2:localhost:5002 3:localhost:5003\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    uint32_t node_id = std::stoul(argv[1]);
    uint16_t port = std::stoul(argv[2]);

    spdlog::set_pattern("[%H:%M:%S.%e] [%^%l%$] %v");
    spdlog::set_level(spdlog::level::info);

    // Parse peer addresses: format "peer_id:host:port"
    std::vector<raftkv::PeerInfo> peers;
    for (int i = 3; i < argc; i++) {
        std::string arg = argv[i];
        auto first_colon = arg.find(':');
        auto last_colon = arg.rfind(':');
        if (first_colon == std::string::npos || first_colon == last_colon) {
            spdlog::error("Invalid peer format: {} (expected id:host:port)", arg);
            return 1;
        }
        raftkv::PeerInfo peer;
        peer.id = std::stoul(arg.substr(0, first_colon));
        peer.host = arg.substr(first_colon + 1, last_colon - first_colon - 1);
        peer.port = std::stoul(arg.substr(last_colon + 1));
        peers.push_back(peer);
    }

    spdlog::info("[node{}] Starting on port {} with {} peers", node_id, port, peers.size());

    // Outbound connections to peers (for sending Raft RPCs)
    // Map peer_id -> TcpClient
    std::mutex peer_clients_mu;
    std::unordered_map<uint32_t, std::unique_ptr<raftkv::TcpClient>> peer_clients;

    // Raft config
    raftkv::RaftConfig config;
    config.node_id = node_id;
    config.listen_port = port;
    config.peers = peers;
    config.log_path = "./data/node" + std::to_string(node_id) + "/";

    // Create RaftNode with a send function that uses outbound TCP connections
    auto raft = std::make_shared<raftkv::RaftNode>(config,
        [&peer_clients, &peer_clients_mu, node_id](uint32_t peer_id, const raftkv::RaftMessage& msg) {
            std::lock_guard lock(peer_clients_mu);
            auto it = peer_clients.find(peer_id);
            if (it != peer_clients.end() && it->second->is_connected()) {
                it->second->send(msg);
            } else {
                spdlog::warn("[node{}] No connection to peer {}", node_id, peer_id);
            }
        });

    // Map of inbound fd -> peer_id (learned from sender_id in messages)
    std::mutex fd_map_mu;
    std::unordered_map<int, uint32_t> fd_to_peer;

    // TCP server: accepts inbound connections from peers and clients
    raftkv::TcpServer server(port,
        [&raft, &server, &fd_map_mu, &fd_to_peer](int fd, const raftkv::RaftMessage& msg) {
            // Track which peer is on which fd
            if (msg.sender_id() != 0) {
                std::lock_guard lock(fd_map_mu);
                fd_to_peer[fd] = msg.sender_id();
            }

            // Create a reply function that sends back on this fd
            auto reply = [&server, fd](const raftkv::RaftMessage& resp) {
                server.send(fd, resp);
            };

            raft->handle_message(msg, reply);
        },
        [&fd_map_mu, &fd_to_peer](int fd) {
            std::lock_guard lock(fd_map_mu);
            fd_to_peer.erase(fd);
        });

    server.start();

    // Give other nodes a moment to start their servers
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Connect outbound to each peer
    for (auto& peer : peers) {
        auto client = std::make_unique<raftkv::TcpClient>(
            peer.host, peer.port,
            [&raft](const raftkv::RaftMessage& msg) {
                // Responses from peers (e.g., RequestVoteResponse, AppendEntriesResponse)
                raft->handle_message(msg, nullptr);
            });

        if (client->connect()) {
            spdlog::info("[node{}] Connected to peer {} at {}:{}",
                         node_id, peer.id, peer.host, peer.port);
        } else {
            spdlog::warn("[node{}] Failed to connect to peer {} at {}:{}",
                         node_id, peer.id, peer.host, peer.port);
        }

        std::lock_guard lock(peer_clients_mu);
        peer_clients[peer.id] = std::move(client);
    }

    // Start Raft (begins election timer)
    raft->start();

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    spdlog::info("[node{}] Running. Press Ctrl+C to stop.", node_id);

    // Reconnect loop: periodically try to reconnect to disconnected peers
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(2));

        std::lock_guard lock(peer_clients_mu);
        for (auto& peer : peers) {
            auto it = peer_clients.find(peer.id);
            if (it == peer_clients.end() || !it->second->is_connected()) {
                auto client = std::make_unique<raftkv::TcpClient>(
                    peer.host, peer.port,
                    [&raft](const raftkv::RaftMessage& msg) {
                        raft->handle_message(msg, nullptr);
                    });
                if (client->connect()) {
                    spdlog::info("[node{}] Reconnected to peer {}", node_id, peer.id);
                    peer_clients[peer.id] = std::move(client);
                }
            }
        }
    }

    spdlog::info("[node{}] Shutting down", node_id);
    raft->stop();
    {
        std::lock_guard lock(peer_clients_mu);
        peer_clients.clear();
    }
    server.stop();
    return 0;
}
