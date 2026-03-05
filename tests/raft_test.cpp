#include "raft/raft_node.h"
#include <spdlog/spdlog.h>
#include <gtest/gtest.h>
#include <chrono>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

using namespace raftkv;

// Helper: create a 3-node in-process Raft cluster with direct message passing
class RaftCluster {
public:
    RaftCluster(int node_count = 3,
                uint32_t election_min_ms = 150,
                uint32_t election_max_ms = 300) {
        // Build configs
        for (int i = 1; i <= node_count; i++) {
            RaftConfig config;
            config.node_id = i;
            config.listen_port = 0; // Not used in tests
            config.election_timeout_min_ms = election_min_ms;
            config.election_timeout_max_ms = election_max_ms;
            config.heartbeat_interval_ms = 50;
            config.log_path = "./data/test_node" + std::to_string(i) + "/";

            for (int j = 1; j <= node_count; j++) {
                if (j == i) continue;
                PeerInfo peer;
                peer.id = j;
                peer.host = "localhost";
                peer.port = 0;
                config.peers.push_back(peer);
            }
            configs_.push_back(config);
        }

        // Create nodes with send functions that route through this cluster
        for (int i = 0; i < node_count; i++) {
            uint32_t node_id = configs_[i].node_id;
            auto node = std::make_shared<RaftNode>(configs_[i],
                [this, node_id](uint32_t peer_id, const RaftMessage& msg) {
                    deliver(node_id, peer_id, msg);
                });
            nodes_[node_id] = node;
        }
    }

    void start_all() {
        for (auto& [id, node] : nodes_) {
            node->start();
        }
    }

    void stop_all() {
        for (auto& [id, node] : nodes_) {
            node->stop();
        }
    }

    void disconnect(uint32_t node_id) {
        std::lock_guard lock(mu_);
        disconnected_.insert(node_id);
    }

    void reconnect(uint32_t node_id) {
        std::lock_guard lock(mu_);
        disconnected_.erase(node_id);
    }

    std::shared_ptr<RaftNode> node(uint32_t id) { return nodes_[id]; }

    // Wait for a leader to be elected, return leader id (0 if timeout)
    uint32_t wait_for_leader(int timeout_ms = 3000) {
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            for (auto& [id, node] : nodes_) {
                if (node->role() == NodeRole::LEADER) return id;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return 0;
    }

    // Count leaders in current term
    int leader_count() {
        int count = 0;
        for (auto& [id, node] : nodes_) {
            if (node->role() == NodeRole::LEADER) count++;
        }
        return count;
    }

private:
    void deliver(uint32_t from, uint32_t to, const RaftMessage& msg) {
        std::lock_guard lock(mu_);
        if (disconnected_.count(from) || disconnected_.count(to)) return;

        auto it = nodes_.find(to);
        if (it != nodes_.end()) {
            // Dispatch asynchronously to avoid deadlocks
            std::thread([node = it->second, msg]() {
                node->handle_message(msg, nullptr);
            }).detach();
        }
    }

    std::vector<RaftConfig> configs_;
    std::unordered_map<uint32_t, std::shared_ptr<RaftNode>> nodes_;
    std::mutex mu_;
    std::set<uint32_t> disconnected_;
};

TEST(RaftTest, LeaderElection) {
    RaftCluster cluster(3, 150, 300);
    cluster.start_all();

    uint32_t leader_id = cluster.wait_for_leader(5000);
    ASSERT_NE(leader_id, 0u) << "No leader elected within timeout";

    // Exactly one leader
    EXPECT_EQ(cluster.leader_count(), 1);

    // Leader should be in a term >= 1
    EXPECT_GE(cluster.node(leader_id)->current_term(), 1u);

    spdlog::info("Leader elected: node {} in term {}",
                 leader_id, cluster.node(leader_id)->current_term());

    cluster.stop_all();
}

TEST(RaftTest, LeaderFailoverElectsNewLeader) {
    RaftCluster cluster(3, 150, 300);
    cluster.start_all();

    uint32_t leader1 = cluster.wait_for_leader(5000);
    ASSERT_NE(leader1, 0u);
    uint64_t term1 = cluster.node(leader1)->current_term();

    spdlog::info("Initial leader: node {} term {}", leader1, term1);

    // Disconnect the leader
    cluster.disconnect(leader1);

    // Wait for a new leader
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    uint32_t leader2 = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        for (uint32_t id = 1; id <= 3; id++) {
            if (id == leader1) continue;
            if (cluster.node(id)->role() == NodeRole::LEADER) {
                leader2 = id;
                break;
            }
        }
        if (leader2 != 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    ASSERT_NE(leader2, 0u) << "No new leader elected after old leader disconnected";
    EXPECT_NE(leader2, leader1);
    EXPECT_GT(cluster.node(leader2)->current_term(), term1);

    spdlog::info("New leader: node {} term {}", leader2,
                 cluster.node(leader2)->current_term());

    cluster.stop_all();
}

TEST(RaftTest, NoSplitBrain) {
    // With 3 nodes and no partition, there should never be two leaders in the same term
    RaftCluster cluster(3, 150, 300);
    cluster.start_all();

    cluster.wait_for_leader(5000);

    // Check multiple times
    for (int i = 0; i < 10; i++) {
        EXPECT_LE(cluster.leader_count(), 1)
            << "Split brain detected at check " << i;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    cluster.stop_all();
}

TEST(RaftTest, LogReplication) {
    RaftCluster cluster(3, 150, 300);
    cluster.start_all();

    uint32_t leader_id = cluster.wait_for_leader(5000);
    ASSERT_NE(leader_id, 0u);

    // Submit a client request to the leader
    RaftMessage client_msg;
    client_msg.set_type(RaftMessage::CLIENT_REQ);
    client_msg.set_sender_id(0);
    auto* req = client_msg.mutable_client_req();
    req->set_command("PUT testkey testvalue");

    bool got_response = false;
    ClientResponse client_response;

    cluster.node(leader_id)->handle_message(client_msg,
        [&](const RaftMessage& resp) {
            if (resp.has_client_resp()) {
                client_response = resp.client_resp();
                got_response = true;
            }
        });

    // Wait for commit and response
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!got_response && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    ASSERT_TRUE(got_response) << "No response from leader for PUT";
    EXPECT_TRUE(client_response.success());

    // Verify commit index advanced
    EXPECT_GE(cluster.node(leader_id)->commit_index(), 1u);

    spdlog::info("PUT committed at index {}", cluster.node(leader_id)->commit_index());

    cluster.stop_all();
}

TEST(RaftTest, MajorityLossStopsCommits) {
    RaftCluster cluster(3, 150, 300);
    cluster.start_all();

    uint32_t leader_id = cluster.wait_for_leader(5000);
    ASSERT_NE(leader_id, 0u);

    // Disconnect both followers (majority lost)
    for (uint32_t id = 1; id <= 3; id++) {
        if (id != leader_id) cluster.disconnect(id);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Submit a write — it should NOT get committed
    RaftMessage client_msg;
    client_msg.set_type(RaftMessage::CLIENT_REQ);
    client_msg.set_sender_id(0);
    auto* req = client_msg.mutable_client_req();
    req->set_command("PUT blocked_key blocked_value");

    bool got_response = false;
    cluster.node(leader_id)->handle_message(client_msg,
        [&](const RaftMessage& resp) {
            got_response = true;
        });

    // Wait a bit — should NOT get committed without majority
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_FALSE(got_response) << "Write committed without majority — safety violation!";

    cluster.stop_all();
}
