#pragma once

#include "raft.pb.h"
#include "kv/state_machine.h"
#include "storage/log_store.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace raftkv {

enum class NodeRole { FOLLOWER, CANDIDATE, LEADER };

struct PeerInfo {
    uint32_t id;
    std::string host;
    uint16_t port;
};

struct RaftConfig {
    uint32_t node_id;
    uint16_t listen_port;
    std::vector<PeerInfo> peers;
    uint32_t election_timeout_min_ms = 150;
    uint32_t election_timeout_max_ms = 300;
    uint32_t heartbeat_interval_ms = 50;
    std::string log_path = "./data/";
    bool persist = false;  // Enable WAL + metadata persistence
};

using SendFunction = std::function<void(uint32_t peer_id, const RaftMessage& msg)>;

class RaftNode {
public:
    RaftNode(const RaftConfig& config, SendFunction send_fn);
    ~RaftNode();

    void start();
    void stop();

    void handle_message(const RaftMessage& msg,
                        std::function<void(const RaftMessage&)> reply = nullptr);

    uint32_t id() const { return config_.node_id; }
    NodeRole role() const { return role_; }
    uint64_t current_term() const { return current_term_; }
    uint32_t leader_id() const { return leader_id_; }
    uint64_t commit_index() const { return commit_index_; }
    uint64_t last_applied() const { return last_applied_; }

private:
    void tick_loop();

    void reset_election_timer();
    void start_election();
    void become_follower(uint64_t term);
    void become_leader();
    void persist_metadata();

    void handle_request_vote(const RequestVoteRequest& req, uint32_t sender,
                             std::function<void(const RaftMessage&)> reply);
    void handle_request_vote_response(const RequestVoteResponse& resp, uint32_t sender);

    void send_append_entries(uint32_t peer_id);
    void send_heartbeats();
    void handle_append_entries(const AppendEntriesRequest& req, uint32_t sender,
                               std::function<void(const RaftMessage&)> reply);
    void handle_append_entries_response(const AppendEntriesResponse& resp, uint32_t sender);

    void advance_commit_index();
    void apply_committed_entries();

    void handle_client_request(const ClientRequest& req,
                               std::function<void(const RaftMessage&)> reply);

    uint64_t last_log_index() const;
    uint64_t last_log_term() const;
    uint64_t log_term(uint64_t index) const;
    int majority_count() const;

    RaftConfig config_;
    SendFunction send_fn_;

    mutable std::recursive_mutex mu_;
    NodeRole role_ = NodeRole::FOLLOWER;
    uint64_t current_term_ = 0;
    uint32_t voted_for_ = 0;
    uint32_t leader_id_ = 0;

    LogStore log_;
    MetadataStore metadata_;

    StateMachine state_machine_;
    uint64_t commit_index_ = 0;
    uint64_t last_applied_ = 0;

    std::unordered_map<uint32_t, uint64_t> next_index_;
    std::unordered_map<uint32_t, uint64_t> match_index_;
    std::unordered_map<uint32_t, bool> votes_received_;
    std::unordered_map<uint64_t, std::function<void(const ClientResponse&)>> pending_requests_;

    std::atomic<bool> running_{false};
    std::thread tick_thread_;

    std::chrono::steady_clock::time_point election_deadline_;
    std::chrono::steady_clock::time_point next_heartbeat_;

    std::mt19937 rng_;
};

}  // namespace raftkv
