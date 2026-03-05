#include "storage/log_store.h"
#include <stdexcept>

namespace raftkv {

LogStore::LogStore(const std::string& path) : path_(path) {}

void LogStore::append(const LogEntry& entry) {
    entries_.push_back(entry);
}

LogEntry LogStore::get(uint64_t index) const {
    if (index == 0 || index > entries_.size())
        throw std::out_of_range("Log index out of range");
    return entries_[index - 1];  // 1-indexed
}

uint64_t LogStore::last_index() const {
    return entries_.size();
}

uint64_t LogStore::last_term() const {
    if (entries_.empty()) return 0;
    return entries_.back().term();
}

void LogStore::truncate_from(uint64_t index) {
    if (index == 0 || index > entries_.size()) return;
    entries_.erase(entries_.begin() + static_cast<long>(index - 1), entries_.end());
}

}  // namespace raftkv
