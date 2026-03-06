#include "raft/raft_node.h"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <thread>
#include <vector>

using namespace raftkv;
using Clock = std::chrono::steady_clock;

// In-process cluster for benchmarking (zero network overhead)
class BenchCluster {
public:
    BenchCluster(int node_count = 3) {
        for (int i = 1; i <= node_count; i++) {
            RaftConfig config;
            config.node_id = i;
            config.listen_port = 0;
            config.election_timeout_min_ms = 150;
            config.election_timeout_max_ms = 300;
            config.heartbeat_interval_ms = 50;

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

    void start() { for (auto& [id, node] : nodes_) node->start(); }
    void stop() { for (auto& [id, node] : nodes_) node->stop(); }

    uint32_t wait_leader(int ms = 5000) {
        auto deadline = Clock::now() + std::chrono::milliseconds(ms);
        while (Clock::now() < deadline) {
            for (auto& [id, node] : nodes_) {
                if (node->role() == NodeRole::LEADER) return id;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return 0;
    }

    std::shared_ptr<RaftNode> node(uint32_t id) { return nodes_[id]; }

private:
    void deliver(uint32_t from, uint32_t to, const RaftMessage& msg) {
        auto it = nodes_.find(to);
        if (it != nodes_.end()) {
            std::thread([node = it->second, msg]() {
                node->handle_message(msg, nullptr);
            }).detach();
        }
    }

    std::unordered_map<uint32_t, std::shared_ptr<RaftNode>> nodes_;
};

struct BenchResult {
    int total_ops;
    double elapsed_sec;
    double ops_per_sec;
    double avg_us;
    double p50_us;
    double p95_us;
    double p99_us;
    double min_us;
    double max_us;
};

double to_us(std::chrono::nanoseconds ns) {
    return static_cast<double>(ns.count()) / 1000.0;
}

BenchResult run_write_bench(BenchCluster& cluster, uint32_t leader_id, int num_ops) {
    std::vector<std::chrono::nanoseconds> latencies;
    latencies.reserve(num_ops);

    auto total_start = Clock::now();

    for (int i = 0; i < num_ops; i++) {
        RaftMessage msg;
        msg.set_type(RaftMessage::CLIENT_REQ);
        msg.set_sender_id(0);
        msg.mutable_client_req()->set_command(
            "PUT bench_key_" + std::to_string(i) + " bench_value_" + std::to_string(i));

        std::mutex mu;
        std::condition_variable cv;
        bool done = false;

        auto op_start = Clock::now();

        cluster.node(leader_id)->handle_message(msg,
            [&](const RaftMessage& resp) {
                std::lock_guard lock(mu);
                done = true;
                cv.notify_one();
            });

        std::unique_lock lock(mu);
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return done; });

        auto op_end = Clock::now();
        if (done) {
            latencies.push_back(op_end - op_start);
        }
    }

    auto total_end = Clock::now();
    double elapsed = std::chrono::duration<double>(total_end - total_start).count();

    // Sort for percentiles
    std::sort(latencies.begin(), latencies.end());

    int n = static_cast<int>(latencies.size());
    double sum = 0;
    for (auto& l : latencies) sum += to_us(l);

    BenchResult r;
    r.total_ops = n;
    r.elapsed_sec = elapsed;
    r.ops_per_sec = n / elapsed;
    r.avg_us = sum / n;
    r.p50_us = to_us(latencies[n * 50 / 100]);
    r.p95_us = to_us(latencies[n * 95 / 100]);
    r.p99_us = to_us(latencies[n * 99 / 100]);
    r.min_us = to_us(latencies.front());
    r.max_us = to_us(latencies.back());
    return r;
}

BenchResult run_read_bench(BenchCluster& cluster, uint32_t leader_id, int num_ops) {
    std::vector<std::chrono::nanoseconds> latencies;
    latencies.reserve(num_ops);

    // Pre-populate keys
    for (int i = 0; i < 100; i++) {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;

        RaftMessage msg;
        msg.set_type(RaftMessage::CLIENT_REQ);
        msg.set_sender_id(0);
        msg.mutable_client_req()->set_command(
            "PUT read_key_" + std::to_string(i) + " read_value_" + std::to_string(i));

        cluster.node(leader_id)->handle_message(msg,
            [&](const RaftMessage& resp) {
                std::lock_guard lock(mu);
                done = true;
                cv.notify_one();
            });

        std::unique_lock lock(mu);
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return done; });
    }

    auto total_start = Clock::now();

    for (int i = 0; i < num_ops; i++) {
        RaftMessage msg;
        msg.set_type(RaftMessage::CLIENT_REQ);
        msg.set_sender_id(0);
        msg.mutable_client_req()->set_command("GET read_key_" + std::to_string(i % 100));

        std::mutex mu;
        std::condition_variable cv;
        bool done = false;

        auto op_start = Clock::now();

        cluster.node(leader_id)->handle_message(msg,
            [&](const RaftMessage& resp) {
                std::lock_guard lock(mu);
                done = true;
                cv.notify_one();
            });

        std::unique_lock lock(mu);
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return done; });

        auto op_end = Clock::now();
        if (done) {
            latencies.push_back(op_end - op_start);
        }
    }

    auto total_end = Clock::now();
    double elapsed = std::chrono::duration<double>(total_end - total_start).count();

    std::sort(latencies.begin(), latencies.end());

    int n = static_cast<int>(latencies.size());
    double sum = 0;
    for (auto& l : latencies) sum += to_us(l);

    BenchResult r;
    r.total_ops = n;
    r.elapsed_sec = elapsed;
    r.ops_per_sec = n / elapsed;
    r.avg_us = sum / n;
    r.p50_us = to_us(latencies[n * 50 / 100]);
    r.p95_us = to_us(latencies[n * 95 / 100]);
    r.p99_us = to_us(latencies[n * 99 / 100]);
    r.min_us = to_us(latencies.front());
    r.max_us = to_us(latencies.back());
    return r;
}

BenchResult run_mixed_bench(BenchCluster& cluster, uint32_t leader_id, int num_ops, int read_pct) {
    // Pre-populate
    for (int i = 0; i < 100; i++) {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;

        RaftMessage msg;
        msg.set_type(RaftMessage::CLIENT_REQ);
        msg.set_sender_id(0);
        msg.mutable_client_req()->set_command(
            "PUT mix_key_" + std::to_string(i) + " mix_value_" + std::to_string(i));

        cluster.node(leader_id)->handle_message(msg,
            [&](const RaftMessage& resp) {
                std::lock_guard lock(mu);
                done = true;
                cv.notify_one();
            });

        std::unique_lock lock(mu);
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return done; });
    }

    std::vector<std::chrono::nanoseconds> latencies;
    latencies.reserve(num_ops);

    auto total_start = Clock::now();

    for (int i = 0; i < num_ops; i++) {
        RaftMessage msg;
        msg.set_type(RaftMessage::CLIENT_REQ);
        msg.set_sender_id(0);

        bool is_read = (i % 100) < read_pct;
        if (is_read) {
            msg.mutable_client_req()->set_command("GET mix_key_" + std::to_string(i % 100));
        } else {
            msg.mutable_client_req()->set_command(
                "PUT mix_key_" + std::to_string(i % 100) + " updated_" + std::to_string(i));
        }

        std::mutex mu;
        std::condition_variable cv;
        bool done = false;

        auto op_start = Clock::now();

        cluster.node(leader_id)->handle_message(msg,
            [&](const RaftMessage& resp) {
                std::lock_guard lock(mu);
                done = true;
                cv.notify_one();
            });

        std::unique_lock lock(mu);
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return done; });

        auto op_end = Clock::now();
        if (done) {
            latencies.push_back(op_end - op_start);
        }
    }

    auto total_end = Clock::now();
    double elapsed = std::chrono::duration<double>(total_end - total_start).count();

    std::sort(latencies.begin(), latencies.end());

    int n = static_cast<int>(latencies.size());
    double sum = 0;
    for (auto& l : latencies) sum += to_us(l);

    BenchResult r;
    r.total_ops = n;
    r.elapsed_sec = elapsed;
    r.ops_per_sec = n / elapsed;
    r.avg_us = sum / n;
    r.p50_us = to_us(latencies[n * 50 / 100]);
    r.p95_us = to_us(latencies[n * 95 / 100]);
    r.p99_us = to_us(latencies[n * 99 / 100]);
    r.min_us = to_us(latencies.front());
    r.max_us = to_us(latencies.back());
    return r;
}

BenchResult run_concurrent_write_bench(BenchCluster& cluster, uint32_t leader_id,
                                        int num_ops, int num_threads) {
    std::atomic<int> completed{0};
    std::vector<std::vector<std::chrono::nanoseconds>> thread_latencies(num_threads);
    int ops_per_thread = num_ops / num_threads;

    auto total_start = Clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            thread_latencies[t].reserve(ops_per_thread);
            for (int i = 0; i < ops_per_thread; i++) {
                int key_id = t * ops_per_thread + i;
                RaftMessage msg;
                msg.set_type(RaftMessage::CLIENT_REQ);
                msg.set_sender_id(0);
                msg.mutable_client_req()->set_command(
                    "PUT cw_" + std::to_string(key_id) + " v" + std::to_string(key_id));

                std::mutex mu;
                std::condition_variable cv;
                bool done = false;

                auto op_start = Clock::now();
                cluster.node(leader_id)->handle_message(msg,
                    [&](const RaftMessage&) {
                        std::lock_guard l(mu);
                        done = true;
                        cv.notify_one();
                    });

                std::unique_lock lock(mu);
                cv.wait_for(lock, std::chrono::seconds(5), [&] { return done; });
                if (done) {
                    thread_latencies[t].push_back(Clock::now() - op_start);
                    completed++;
                }
            }
        });
    }

    for (auto& t : threads) t.join();
    auto total_end = Clock::now();
    double elapsed = std::chrono::duration<double>(total_end - total_start).count();

    // Merge all latencies
    std::vector<std::chrono::nanoseconds> latencies;
    for (auto& tl : thread_latencies) {
        latencies.insert(latencies.end(), tl.begin(), tl.end());
    }
    std::sort(latencies.begin(), latencies.end());

    int n = static_cast<int>(latencies.size());
    double sum = 0;
    for (auto& l : latencies) sum += to_us(l);

    BenchResult r;
    r.total_ops = n;
    r.elapsed_sec = elapsed;
    r.ops_per_sec = n / elapsed;
    r.avg_us = sum / n;
    r.p50_us = to_us(latencies[n * 50 / 100]);
    r.p95_us = to_us(latencies[n * 95 / 100]);
    r.p99_us = to_us(latencies[n * 99 / 100]);
    r.min_us = to_us(latencies.front());
    r.max_us = to_us(latencies.back());
    return r;
}

void print_result(const std::string& name, const BenchResult& r) {
    std::cout << "\n" << name << "\n";
    std::cout << std::string(60, '-') << "\n";
    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  Ops completed:  " << r.total_ops << "\n";
    std::cout << "  Elapsed:        " << std::setprecision(3) << r.elapsed_sec << " sec\n";
    std::cout << "  Throughput:     " << std::setprecision(0) << r.ops_per_sec << " ops/sec\n";
    std::cout << std::setprecision(1);
    std::cout << "  Latency (us):   avg=" << r.avg_us
              << "  p50=" << r.p50_us
              << "  p95=" << r.p95_us
              << "  p99=" << r.p99_us << "\n";
    std::cout << "                  min=" << r.min_us
              << "  max=" << r.max_us << "\n";
}

int main(int argc, char* argv[]) {
    spdlog::set_level(spdlog::level::warn); // Quiet during benchmarks

    int num_write_ops = 1000;
    int num_read_ops = 5000;
    int num_mixed_ops = 2000;

    if (argc > 1) num_write_ops = std::atoi(argv[1]);
    if (argc > 2) num_read_ops = std::atoi(argv[2]);
    if (argc > 3) num_mixed_ops = std::atoi(argv[3]);

    std::cout << "========================================\n";
    std::cout << "  RaftKV Consensus Benchmark\n";
    std::cout << "  3-node in-process cluster\n";
    std::cout << "========================================\n";

    BenchCluster cluster(3);
    cluster.start();

    uint32_t leader = cluster.wait_leader(5000);
    if (leader == 0) {
        std::cerr << "Failed to elect leader\n";
        return 1;
    }
    std::cout << "\nLeader elected: node " << leader << "\n";

    // Warmup
    std::cout << "Warming up..." << std::flush;
    for (int i = 0; i < 50; i++) {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;
        RaftMessage msg;
        msg.set_type(RaftMessage::CLIENT_REQ);
        msg.set_sender_id(0);
        msg.mutable_client_req()->set_command("PUT warmup_" + std::to_string(i) + " w");
        cluster.node(leader)->handle_message(msg,
            [&](const RaftMessage&) { std::lock_guard l(mu); done = true; cv.notify_one(); });
        std::unique_lock lock(mu);
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return done; });
    }
    std::cout << " done\n";

    // Run benchmarks
    auto write_result = run_write_bench(cluster, leader, num_write_ops);
    print_result("WRITE (PUT) - Replicated Consensus", write_result);

    auto read_result = run_read_bench(cluster, leader, num_read_ops);
    print_result("READ (GET) - Leader Local", read_result);

    auto mixed_result = run_mixed_bench(cluster, leader, num_mixed_ops, 80);
    print_result("MIXED (80% read / 20% write)", mixed_result);

    auto concurrent_result = run_concurrent_write_bench(cluster, leader, num_write_ops, 4);
    print_result("CONCURRENT WRITE (4 threads)", concurrent_result);

    std::cout << "\n========================================\n";
    std::cout << "  Summary\n";
    std::cout << "========================================\n";
    std::cout << std::fixed << std::setprecision(0);
    std::cout << "  Write throughput:       " << write_result.ops_per_sec << " ops/sec\n";
    std::cout << "  Read throughput:        " << read_result.ops_per_sec << " ops/sec\n";
    std::cout << "  Mixed throughput:       " << mixed_result.ops_per_sec << " ops/sec\n";
    std::cout << "  Concurrent write (4T):  " << concurrent_result.ops_per_sec << " ops/sec\n";
    std::cout << std::setprecision(1);
    std::cout << "  Write p99 latency:      " << write_result.p99_us << " us\n";
    std::cout << "  Read p99 latency:       " << read_result.p99_us << " us\n";
    std::cout << "  Concurrent p99 latency: " << concurrent_result.p99_us << " us\n";
    std::cout << "\n";

    cluster.stop();
    return 0;
}
