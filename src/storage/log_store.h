#pragma once

#include "raft.pb.h"
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

namespace raftkv {

// WAL record format (per entry):
//   [4 bytes] payload_length (little-endian)
//   [N bytes] serialized LogEntry protobuf
//   [4 bytes] CRC32 of the payload bytes

class LogStore {
public:
    // If persist=true, uses WAL at path/wal.bin. If false, in-memory only (tests).
    explicit LogStore(const std::string& path, bool persist = false);
    ~LogStore();

    void append(const LogEntry& entry);
    const LogEntry& get(uint64_t index) const;
    uint64_t last_index() const;
    uint64_t last_term() const;
    void truncate_from(uint64_t index);
    size_t size() const { return entries_.size(); }

private:
    void recover();
    void write_entry_to_wal(const LogEntry& entry);
    void rewrite_wal();

    std::string path_;
    bool persist_;
    std::ofstream wal_file_;
    std::vector<LogEntry> entries_;
};

// Persists currentTerm and votedFor to a simple binary file.
// Format: [8 bytes term][4 bytes votedFor][4 bytes CRC32]
class MetadataStore {
public:
    explicit MetadataStore(const std::string& path, bool persist = false);

    void save(uint64_t term, uint32_t voted_for);
    void load(uint64_t& term, uint32_t& voted_for);

private:
    std::string filepath_;
    bool persist_;
};

}  // namespace raftkv
