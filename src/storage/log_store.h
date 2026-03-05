#pragma once

#include "raft.pb.h"
#include <cstdint>
#include <string>
#include <vector>

namespace raftkv {

class LogStore {
public:
    explicit LogStore(const std::string& path);

    void append(const LogEntry& entry);
    LogEntry get(uint64_t index) const;
    uint64_t last_index() const;
    uint64_t last_term() const;

    // Truncate all entries from the given index onwards (inclusive)
    void truncate_from(uint64_t index);

private:
    std::string path_;
    std::vector<LogEntry> entries_;
};

}  // namespace raftkv
