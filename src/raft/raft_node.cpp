#include "raft/raft_node.h"
#include <spdlog/spdlog.h>
#include <algorithm>

namespace raftkv {

RaftNode::RaftNode(const RaftConfig& config, SendFunction send_fn)
    : config_(config),
      send_fn_(std::move(send_fn)),
      log_(config.log_path, config.persist),
      metadata_(config.log_path, config.persist),
      rng_(std::random_device{}()) {
    // Recover persisted state
    metadata_.load(current_term_, voted_for_);
    if (current_term_ > 0 || voted_for_ > 0) {
        spdlog::info("[node{}] Recovered: term={}, votedFor={}, log_size={}",
                     id(), current_term_, voted_for_, log_.last_index());
    }
}

RaftNode::~RaftNode() { stop(); }

void RaftNode::start() {
    // Replay committed log entries to rebuild state machine.
    // On recovery we don't know commit_index yet (it's not persisted),
    // but all entries in our log were accepted, so we conservatively
    // apply them all. The Raft leader will correct our commit_index.
    // This is safe because the state machine is deterministic.
    if (log_.last_index() > 0 && last_applied_ == 0) {
        commit_index_ = log_.last_index();
        apply_committed_entries();
        spdlog::info("[node{}] Replayed {} log entries to state machine",
                     id(), last_applied_);
    }

    running_ = true;
    reset_election_timer();
    next_heartbeat_ = std::chrono::steady_clock::now();
    tick_thread_ = std::thread(&RaftNode::tick_loop, this);
    spdlog::info("[node{}] Raft started as follower, term={}", id(), current_term_);
}

void RaftNode::stop() {
    running_ = false;
    if (tick_thread_.joinable()) tick_thread_.join();
}

void RaftNode::persist_metadata() {
    metadata_.save(current_term_, voted_for_);
}

// --- Single tick loop ---

void RaftNode::tick_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (!running_) break;

        auto now = std::chrono::steady_clock::now();
        std::lock_guard lock(mu_);

        if (role_ == NodeRole::LEADER) {
            if (now >= next_heartbeat_) {
                for (auto& peer : config_.peers) {
                    send_append_entries(peer.id);
                }
                next_heartbeat_ = now + std::chrono::milliseconds(config_.heartbeat_interval_ms);
            }
        } else {
            if (now >= election_deadline_) {
                spdlog::info("[node{}] Election timeout, starting election", id());
                start_election();
            }
        }
    }
}

// --- Election ---

void RaftNode::reset_election_timer() {
    std::uniform_int_distribution<uint32_t> dist(
        config_.election_timeout_min_ms, config_.election_timeout_max_ms);
    election_deadline_ = std::chrono::steady_clock::now() +
                         std::chrono::milliseconds(dist(rng_));
}

void RaftNode::start_election() {
    current_term_++;
    role_ = NodeRole::CANDIDATE;
    voted_for_ = id();
    leader_id_ = 0;
    votes_received_.clear();
    votes_received_[id()] = true;
    persist_metadata();

    spdlog::info("[node{}] Became candidate for term {}", id(), current_term_);

    if (majority_count() == 1) {
        become_leader();
        return;
    }

    uint64_t term = current_term_;
    uint64_t last_idx = last_log_index();
    uint64_t last_trm = last_log_term();

    for (auto& peer : config_.peers) {
        RaftMessage msg;
        msg.set_type(RaftMessage::REQUEST_VOTE_REQ);
        msg.set_sender_id(id());
        auto* rv = msg.mutable_request_vote_req();
        rv->set_term(term);
        rv->set_candidate_id(id());
        rv->set_last_log_index(last_idx);
        rv->set_last_log_term(last_trm);
        send_fn_(peer.id, msg);
    }

    reset_election_timer();
}

void RaftNode::become_follower(uint64_t term) {
    if (term > current_term_) {
        current_term_ = term;
        voted_for_ = 0;
        persist_metadata();
    }
    role_ = NodeRole::FOLLOWER;
    reset_election_timer();
}

void RaftNode::become_leader() {
    role_ = NodeRole::LEADER;
    leader_id_ = id();

    uint64_t last_idx = last_log_index();
    next_index_.clear();
    match_index_.clear();
    for (auto& peer : config_.peers) {
        next_index_[peer.id] = last_idx + 1;
        match_index_[peer.id] = 0;
    }

    spdlog::info("[node{}] Became LEADER for term {}", id(), current_term_);

    next_heartbeat_ = std::chrono::steady_clock::now();
    for (auto& peer : config_.peers) {
        send_append_entries(peer.id);
    }
}

// --- RequestVote ---

void RaftNode::handle_request_vote(const RequestVoteRequest& req, uint32_t sender,
                                    std::function<void(const RaftMessage&)> reply) {
    std::lock_guard lock(mu_);

    RaftMessage resp_msg;
    resp_msg.set_type(RaftMessage::REQUEST_VOTE_RESP);
    resp_msg.set_sender_id(id());
    auto* resp = resp_msg.mutable_request_vote_resp();

    if (req.term() > current_term_) {
        become_follower(req.term());
    }

    resp->set_term(current_term_);
    resp->set_vote_granted(false);

    if (req.term() < current_term_) {
        if (reply) reply(resp_msg);
        else send_fn_(sender, resp_msg);
        return;
    }

    bool can_vote = (voted_for_ == 0 || voted_for_ == req.candidate_id());
    bool log_ok = (req.last_log_term() > last_log_term()) ||
                  (req.last_log_term() == last_log_term() &&
                   req.last_log_index() >= last_log_index());

    if (can_vote && log_ok) {
        voted_for_ = req.candidate_id();
        persist_metadata();
        resp->set_vote_granted(true);
        reset_election_timer();
        spdlog::info("[node{}] Granted vote to {} for term {}", id(), sender, req.term());
    }

    if (reply) reply(resp_msg);
    else send_fn_(sender, resp_msg);
}

void RaftNode::handle_request_vote_response(const RequestVoteResponse& resp, uint32_t sender) {
    std::lock_guard lock(mu_);

    if (resp.term() > current_term_) {
        become_follower(resp.term());
        return;
    }

    if (role_ != NodeRole::CANDIDATE || resp.term() != current_term_) return;

    if (resp.vote_granted()) {
        votes_received_[sender] = true;
        spdlog::info("[node{}] Got vote from {} ({}/{} needed)",
                     id(), sender, votes_received_.size(), majority_count());

        if (static_cast<int>(votes_received_.size()) >= majority_count()) {
            become_leader();
        }
    }
}

// --- AppendEntries ---

void RaftNode::send_append_entries(uint32_t peer_id) {
    RaftMessage msg;
    msg.set_type(RaftMessage::APPEND_ENTRIES_REQ);
    msg.set_sender_id(id());
    auto* ae = msg.mutable_append_entries_req();

    ae->set_term(current_term_);
    ae->set_leader_id(id());
    ae->set_leader_commit(commit_index_);

    uint64_t next = next_index_[peer_id];
    uint64_t prev_idx = next - 1;
    ae->set_prev_log_index(prev_idx);
    ae->set_prev_log_term(log_term(prev_idx));

    uint64_t last_idx = last_log_index();
    for (uint64_t i = next; i <= last_idx; i++) {
        *ae->add_entries() = log_.get(i);
    }

    send_fn_(peer_id, msg);
}

void RaftNode::send_heartbeats() {
    std::lock_guard lock(mu_);
    if (role_ != NodeRole::LEADER) return;

    for (auto& peer : config_.peers) {
        send_append_entries(peer.id);
    }
}

void RaftNode::handle_append_entries(const AppendEntriesRequest& req, uint32_t sender,
                                      std::function<void(const RaftMessage&)> reply) {
    std::lock_guard lock(mu_);

    RaftMessage resp_msg;
    resp_msg.set_type(RaftMessage::APPEND_ENTRIES_RESP);
    resp_msg.set_sender_id(id());
    auto* resp = resp_msg.mutable_append_entries_resp();
    resp->set_term(current_term_);
    resp->set_success(false);

    if (req.term() > current_term_) {
        become_follower(req.term());
    }

    if (req.term() < current_term_) {
        resp->set_term(current_term_);
        if (reply) reply(resp_msg);
        else send_fn_(sender, resp_msg);
        return;
    }

    leader_id_ = req.leader_id();
    if (role_ == NodeRole::CANDIDATE) {
        role_ = NodeRole::FOLLOWER;
    }
    reset_election_timer();

    // Log consistency check
    if (req.prev_log_index() > 0) {
        if (req.prev_log_index() > last_log_index()) {
            resp->set_match_index(last_log_index());
            if (reply) reply(resp_msg);
            else send_fn_(sender, resp_msg);
            return;
        }
        if (log_term(req.prev_log_index()) != req.prev_log_term()) {
            log_.truncate_from(req.prev_log_index());
            resp->set_match_index(req.prev_log_index() - 1);
            if (reply) reply(resp_msg);
            else send_fn_(sender, resp_msg);
            return;
        }
    }

    // Append new entries
    uint64_t insert_index = req.prev_log_index() + 1;
    for (int i = 0; i < req.entries_size(); i++) {
        uint64_t idx = insert_index + i;
        if (idx <= last_log_index()) {
            if (log_term(idx) != req.entries(i).term()) {
                log_.truncate_from(idx);
                log_.append(req.entries(i));
            }
        } else {
            log_.append(req.entries(i));
        }
    }

    if (req.leader_commit() > commit_index_) {
        uint64_t new_commit = std::min(req.leader_commit(), last_log_index());
        if (new_commit > commit_index_) {
            commit_index_ = new_commit;
            apply_committed_entries();
        }
    }

    resp->set_success(true);
    resp->set_match_index(last_log_index());
    resp->set_term(current_term_);

    if (reply) reply(resp_msg);
    else send_fn_(sender, resp_msg);
}

void RaftNode::handle_append_entries_response(const AppendEntriesResponse& resp, uint32_t sender) {
    std::lock_guard lock(mu_);

    if (resp.term() > current_term_) {
        become_follower(resp.term());
        return;
    }

    if (role_ != NodeRole::LEADER || resp.term() != current_term_) return;

    if (resp.success()) {
        match_index_[sender] = resp.match_index();
        next_index_[sender] = resp.match_index() + 1;
        advance_commit_index();
    } else {
        if (resp.match_index() > 0) {
            next_index_[sender] = resp.match_index() + 1;
        } else {
            next_index_[sender] = std::max(uint64_t(1), next_index_[sender] - 1);
        }
        send_append_entries(sender);
    }
}

// --- Commit & Apply ---

void RaftNode::advance_commit_index() {
    // O(peers log peers) instead of O(log_size * peers):
    // Sort match indices and pick the median (majority threshold).
    std::vector<uint64_t> indices;
    indices.reserve(config_.peers.size() + 1);
    indices.push_back(last_log_index()); // Leader's own match index
    for (auto& peer : config_.peers) {
        indices.push_back(match_index_[peer.id]);
    }
    std::sort(indices.begin(), indices.end(), std::greater<>());

    // The majority_count()-th largest index is the highest N replicated on a majority
    uint64_t n = indices[majority_count() - 1];
    if (n > commit_index_ && log_term(n) == current_term_) {
        spdlog::info("[node{}] Advancing commit index {} -> {}", id(), commit_index_, n);
        commit_index_ = n;
        apply_committed_entries();
    }
}

void RaftNode::apply_committed_entries() {
    while (last_applied_ < commit_index_) {
        last_applied_++;
        auto entry = log_.get(last_applied_);
        std::string result = state_machine_.apply(entry.command());
        spdlog::info("[node{}] Applied log[{}]: '{}' -> '{}'",
                     id(), last_applied_, entry.command(), result);

        auto it = pending_requests_.find(last_applied_);
        if (it != pending_requests_.end()) {
            ClientResponse client_resp;
            client_resp.set_success(true);
            client_resp.set_value(result);
            it->second(client_resp);
            pending_requests_.erase(it);
        }
    }
}

// --- Client Requests ---

void RaftNode::handle_client_request(const ClientRequest& req,
                                      std::function<void(const RaftMessage&)> reply) {
    std::lock_guard lock(mu_);

    RaftMessage resp_msg;
    resp_msg.set_type(RaftMessage::CLIENT_RESP);
    resp_msg.set_sender_id(id());
    auto* resp = resp_msg.mutable_client_resp();

    if (role_ != NodeRole::LEADER) {
        resp->set_success(false);
        resp->set_error("Not leader");
        resp->set_leader_hint(leader_id_);
        if (reply) reply(resp_msg);
        return;
    }

    // Read-only commands: serve directly from leader's state machine
    std::string cmd = req.command();
    bool is_read = (cmd.size() >= 3 && cmd.substr(0, 3) == "GET") ||
                   (cmd.size() >= 4 && cmd.substr(0, 4) == "LIST");
    if (is_read) {
        std::string result = state_machine_.apply(cmd);
        resp->set_success(true);
        resp->set_value(result);
        if (reply) reply(resp_msg);
        return;
    }

    LogEntry entry;
    entry.set_index(last_log_index() + 1);
    entry.set_term(current_term_);
    entry.set_command(req.command());
    log_.append(entry);

    uint64_t entry_index = last_log_index();
    spdlog::info("[node{}] Appended log[{}] term={}: '{}'",
                 id(), entry_index, current_term_, req.command());

    if (reply) {
        pending_requests_[entry_index] = [reply, this](const ClientResponse& client_resp) {
            RaftMessage resp_msg;
            resp_msg.set_type(RaftMessage::CLIENT_RESP);
            resp_msg.set_sender_id(id());
            *resp_msg.mutable_client_resp() = client_resp;
            reply(resp_msg);
        };
    }

    for (auto& peer : config_.peers) {
        send_append_entries(peer.id);
    }
}

// --- Message Dispatch ---

void RaftNode::handle_message(const RaftMessage& msg,
                               std::function<void(const RaftMessage&)> reply) {
    switch (msg.type()) {
        case RaftMessage::REQUEST_VOTE_REQ:
            handle_request_vote(msg.request_vote_req(), msg.sender_id(), reply);
            break;
        case RaftMessage::REQUEST_VOTE_RESP:
            handle_request_vote_response(msg.request_vote_resp(), msg.sender_id());
            break;
        case RaftMessage::APPEND_ENTRIES_REQ:
            handle_append_entries(msg.append_entries_req(), msg.sender_id(), reply);
            break;
        case RaftMessage::APPEND_ENTRIES_RESP:
            handle_append_entries_response(msg.append_entries_resp(), msg.sender_id());
            break;
        case RaftMessage::CLIENT_REQ:
            handle_client_request(msg.client_req(), reply);
            break;
        default:
            spdlog::warn("[node{}] Unknown message type {}", id(), static_cast<int>(msg.type()));
            break;
    }
}

// --- Helpers ---

uint64_t RaftNode::last_log_index() const {
    return log_.last_index();
}

uint64_t RaftNode::last_log_term() const {
    return log_.last_term();
}

uint64_t RaftNode::log_term(uint64_t index) const {
    if (index == 0 || index > log_.last_index()) return 0;
    return log_.get(index).term();
}

int RaftNode::majority_count() const {
    int total = static_cast<int>(config_.peers.size()) + 1;
    return total / 2 + 1;
}

}  // namespace raftkv
