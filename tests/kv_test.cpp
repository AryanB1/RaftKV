#include "raft/raft_node.h"
#include "kv/state_machine.h"
#include <spdlog/spdlog.h>
#include <gtest/gtest.h>
#include <chrono>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

using namespace raftkv;

// Reusable in-process cluster for KV tests
class KvCluster {
public:
    KvCluster(int node_count = 3) {
        for (int i = 1; i <= node_count; i++) {
            RaftConfig config;
            config.node_id = i;
            config.listen_port = 0;
            config.election_timeout_min_ms = 150;
            config.election_timeout_max_ms = 300;
            config.heartbeat_interval_ms = 50;
            config.log_path = "./data/kvtest_node" + std::to_string(i) + "/";

            for (int j = 1; j <= node_count; j++) {
                if (j == i) continue;
                config.peers.push_back({static_cast<uint32_t>(j), "localhost", 0});
            }

            auto node = std::make_shared<RaftNode>(config,
                [this, i](uint32_t peer_id, const RaftMessage& msg) {
                    deliver(i, peer_id, msg);
                });
            nodes_[i] = node;
        }
    }

    void start() {
        for (auto& [id, node] : nodes_) node->start();
    }

    void stop() {
        for (auto& [id, node] : nodes_) node->stop();
    }

    void disconnect(uint32_t id) {
        std::lock_guard lock(mu_);
        disconnected_.insert(id);
    }

    void reconnect(uint32_t id) {
        std::lock_guard lock(mu_);
        disconnected_.erase(id);
    }

    uint32_t wait_leader(int ms = 5000) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
        while (std::chrono::steady_clock::now() < deadline) {
            for (auto& [id, node] : nodes_) {
                if (node->role() == NodeRole::LEADER) return id;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return 0;
    }

    // Send a client command to the leader and wait for response.
    // Returns {success, value/error}
    std::pair<bool, std::string> execute(const std::string& command, int timeout_ms = 3000) {
        uint32_t leader = wait_leader(timeout_ms);
        if (leader == 0) return {false, "no leader"};

        RaftMessage msg;
        msg.set_type(RaftMessage::CLIENT_REQ);
        msg.set_sender_id(0);
        msg.mutable_client_req()->set_command(command);

        std::mutex mu;
        std::condition_variable cv;
        bool done = false;
        std::string result_value;
        bool result_success = false;

        nodes_[leader]->handle_message(msg,
            [&](const RaftMessage& resp) {
                std::lock_guard lock(mu);
                if (resp.has_client_resp()) {
                    result_success = resp.client_resp().success();
                    result_value = result_success ? resp.client_resp().value()
                                                  : resp.client_resp().error();
                }
                done = true;
                cv.notify_one();
            });

        std::unique_lock lock(mu);
        cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&] { return done; });
        if (!done) return {false, "timeout"};
        return {result_success, result_value};
    }

    std::shared_ptr<RaftNode> node(uint32_t id) { return nodes_[id]; }

private:
    void deliver(uint32_t from, uint32_t to, const RaftMessage& msg) {
        std::lock_guard lock(mu_);
        if (disconnected_.count(from) || disconnected_.count(to)) return;
        auto it = nodes_.find(to);
        if (it != nodes_.end()) {
            std::thread([node = it->second, msg]() {
                node->handle_message(msg, nullptr);
            }).detach();
        }
    }

    std::unordered_map<uint32_t, std::shared_ptr<RaftNode>> nodes_;
    std::mutex mu_;
    std::set<uint32_t> disconnected_;
};

// --- State Machine Unit Tests ---

TEST(StateMachineTest, PutAndGet) {
    StateMachine sm;
    EXPECT_EQ(sm.apply("PUT name aryan"), "OK");
    EXPECT_EQ(sm.apply("GET name"), "aryan");
}

TEST(StateMachineTest, GetMissing) {
    StateMachine sm;
    EXPECT_EQ(sm.apply("GET nonexistent"), "(nil)");
}

TEST(StateMachineTest, Delete) {
    StateMachine sm;
    sm.apply("PUT key1 value1");
    EXPECT_EQ(sm.apply("DELETE key1"), "OK");
    EXPECT_EQ(sm.apply("GET key1"), "(nil)");
}

TEST(StateMachineTest, DeleteMissing) {
    StateMachine sm;
    EXPECT_EQ(sm.apply("DELETE nope"), "(nil)");
}

TEST(StateMachineTest, Overwrite) {
    StateMachine sm;
    sm.apply("PUT key1 first");
    sm.apply("PUT key1 second");
    EXPECT_EQ(sm.apply("GET key1"), "second");
}

TEST(StateMachineTest, List) {
    StateMachine sm;
    EXPECT_EQ(sm.apply("LIST"), "(empty)");
    sm.apply("PUT b val_b");
    sm.apply("PUT a val_a");
    sm.apply("PUT c val_c");
    EXPECT_EQ(sm.apply("LIST"), "a\nb\nc");
}

TEST(StateMachineTest, ValueWithSpaces) {
    StateMachine sm;
    EXPECT_EQ(sm.apply("PUT greeting hello world"), "OK");
    EXPECT_EQ(sm.apply("GET greeting"), "hello world");
}

TEST(StateMachineTest, UnknownCommand) {
    StateMachine sm;
    EXPECT_EQ(sm.apply("FOOBAR"), "ERR unknown command 'FOOBAR'");
}

TEST(StateMachineTest, MissingArgs) {
    StateMachine sm;
    EXPECT_EQ(sm.apply("GET"), "ERR GET requires a key");
    EXPECT_EQ(sm.apply("PUT"), "ERR PUT requires key and value");
    EXPECT_EQ(sm.apply("PUT onlykey"), "ERR PUT requires a value");
    EXPECT_EQ(sm.apply("DELETE"), "ERR DELETE requires a key");
}

// --- KV through Raft Integration Tests ---

TEST(KvRaftTest, PutGetThroughConsensus) {
    KvCluster cluster;
    cluster.start();

    auto [ok1, v1] = cluster.execute("PUT mykey myvalue");
    ASSERT_TRUE(ok1) << v1;
    EXPECT_EQ(v1, "OK");

    auto [ok2, v2] = cluster.execute("GET mykey");
    ASSERT_TRUE(ok2) << v2;
    EXPECT_EQ(v2, "myvalue");

    cluster.stop();
}

TEST(KvRaftTest, MultipleWrites) {
    KvCluster cluster;
    cluster.start();

    for (int i = 0; i < 5; i++) {
        auto [ok, v] = cluster.execute("PUT key" + std::to_string(i) + " val" + std::to_string(i));
        ASSERT_TRUE(ok) << "PUT key" << i << " failed: " << v;
    }

    for (int i = 0; i < 5; i++) {
        auto [ok, v] = cluster.execute("GET key" + std::to_string(i));
        ASSERT_TRUE(ok) << "GET key" << i << " failed: " << v;
        EXPECT_EQ(v, "val" + std::to_string(i));
    }

    cluster.stop();
}

TEST(KvRaftTest, DeleteThroughConsensus) {
    KvCluster cluster;
    cluster.start();

    cluster.execute("PUT delme gone_soon");
    auto [ok1, v1] = cluster.execute("GET delme");
    ASSERT_TRUE(ok1);
    EXPECT_EQ(v1, "gone_soon");

    auto [ok2, v2] = cluster.execute("DELETE delme");
    ASSERT_TRUE(ok2);
    EXPECT_EQ(v2, "OK");

    auto [ok3, v3] = cluster.execute("GET delme");
    ASSERT_TRUE(ok3);
    EXPECT_EQ(v3, "(nil)");

    cluster.stop();
}

TEST(KvRaftTest, ListThroughConsensus) {
    KvCluster cluster;
    cluster.start();

    cluster.execute("PUT zeta z_val");
    cluster.execute("PUT alpha a_val");
    cluster.execute("PUT beta b_val");

    auto [ok, v] = cluster.execute("LIST");
    ASSERT_TRUE(ok) << v;
    EXPECT_EQ(v, "alpha\nbeta\nzeta");

    cluster.stop();
}

TEST(KvRaftTest, NonLeaderReturnsRedirect) {
    KvCluster cluster;
    cluster.start();

    uint32_t leader = cluster.wait_leader();
    ASSERT_NE(leader, 0u);

    // Find a follower
    uint32_t follower = 0;
    for (uint32_t id = 1; id <= 3; id++) {
        if (id != leader) { follower = id; break; }
    }

    // Send request directly to follower
    RaftMessage msg;
    msg.set_type(RaftMessage::CLIENT_REQ);
    msg.set_sender_id(0);
    msg.mutable_client_req()->set_command("GET anything");

    bool got_redirect = false;
    uint32_t hinted_leader = 0;

    cluster.node(follower)->handle_message(msg,
        [&](const RaftMessage& resp) {
            if (resp.has_client_resp() && !resp.client_resp().success()) {
                got_redirect = true;
                hinted_leader = resp.client_resp().leader_hint();
            }
        });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(got_redirect);
    EXPECT_EQ(hinted_leader, leader);

    cluster.stop();
}

TEST(KvRaftTest, WritesSurviveLeaderFailover) {
    KvCluster cluster;
    cluster.start();

    // Write some data
    auto [ok1, _1] = cluster.execute("PUT persist_key persist_value");
    ASSERT_TRUE(ok1);

    // Wait for replication
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Kill the leader
    uint32_t old_leader = cluster.wait_leader();
    cluster.disconnect(old_leader);

    // Wait for new leader
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    uint32_t new_leader = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        for (uint32_t id = 1; id <= 3; id++) {
            if (id == old_leader) continue;
            if (cluster.node(id)->role() == NodeRole::LEADER) {
                new_leader = id;
                break;
            }
        }
        if (new_leader) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_NE(new_leader, 0u);

    // Read from new leader — data should still be there
    auto [ok2, v2] = cluster.execute("GET persist_key");
    ASSERT_TRUE(ok2) << v2;
    EXPECT_EQ(v2, "persist_value");

    cluster.stop();
}
