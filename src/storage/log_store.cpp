#include "storage/log_store.h"
#include <spdlog/spdlog.h>
#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace raftkv {

// Simple CRC32 (IEEE polynomial)
static uint32_t crc32(const uint8_t* data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
        }
    }
    return ~crc;
}

// --- LogStore ---

LogStore::LogStore(const std::string& path, bool persist)
    : path_(path), persist_(persist) {
    if (persist_) {
        std::filesystem::create_directories(path_);
        recover();
        // Open WAL for appending
        wal_file_.open(path_ + "wal.bin",
                       std::ios::binary | std::ios::app);
        if (!wal_file_) {
            spdlog::error("Failed to open WAL at {}", path_ + "wal.bin");
        }
    }
}

LogStore::~LogStore() {
    if (wal_file_.is_open()) wal_file_.close();
}

void LogStore::recover() {
    std::string wal_path = path_ + "wal.bin";
    std::ifstream in(wal_path, std::ios::binary);
    if (!in) return;  // No WAL yet

    entries_.clear();
    while (in.peek() != EOF) {
        // Read length
        uint32_t payload_len = 0;
        in.read(reinterpret_cast<char*>(&payload_len), 4);
        if (!in || payload_len == 0 || payload_len > 64 * 1024 * 1024) break;

        // Read payload
        std::vector<uint8_t> payload(payload_len);
        in.read(reinterpret_cast<char*>(payload.data()), payload_len);
        if (!in) break;

        // Read checksum
        uint32_t stored_crc = 0;
        in.read(reinterpret_cast<char*>(&stored_crc), 4);
        if (!in) break;

        // Verify checksum
        uint32_t computed_crc = crc32(payload.data(), payload_len);
        if (stored_crc != computed_crc) {
            spdlog::warn("WAL corruption detected at entry {}, truncating", entries_.size() + 1);
            break;
        }

        // Parse protobuf
        LogEntry entry;
        if (!entry.ParseFromArray(payload.data(), payload_len)) {
            spdlog::warn("WAL protobuf parse failed at entry {}, truncating", entries_.size() + 1);
            break;
        }

        entries_.push_back(std::move(entry));
    }

    spdlog::info("WAL recovery: loaded {} entries from {}", entries_.size(), wal_path);
}

void LogStore::write_entry_to_wal(const LogEntry& entry) {
    if (!persist_ || !wal_file_.is_open()) return;

    std::string payload = entry.SerializeAsString();
    uint32_t payload_len = static_cast<uint32_t>(payload.size());
    uint32_t checksum = crc32(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

    wal_file_.write(reinterpret_cast<const char*>(&payload_len), 4);
    wal_file_.write(payload.data(), payload.size());
    wal_file_.write(reinterpret_cast<const char*>(&checksum), 4);
    wal_file_.flush();  // fsync equivalent for fstream
}

void LogStore::rewrite_wal() {
    if (!persist_) return;

    // Close current WAL
    if (wal_file_.is_open()) wal_file_.close();

    // Write all current entries to a new WAL
    std::string wal_path = path_ + "wal.bin";
    std::string tmp_path = path_ + "wal.tmp";

    std::ofstream tmp(tmp_path, std::ios::binary | std::ios::trunc);
    for (auto& entry : entries_) {
        std::string payload = entry.SerializeAsString();
        uint32_t payload_len = static_cast<uint32_t>(payload.size());
        uint32_t checksum = crc32(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

        tmp.write(reinterpret_cast<const char*>(&payload_len), 4);
        tmp.write(payload.data(), payload.size());
        tmp.write(reinterpret_cast<const char*>(&checksum), 4);
    }
    tmp.flush();
    tmp.close();

    // Atomic rename
    std::filesystem::rename(tmp_path, wal_path);

    // Reopen for appending
    wal_file_.open(wal_path, std::ios::binary | std::ios::app);
}

void LogStore::append(const LogEntry& entry) {
    entries_.push_back(entry);
    write_entry_to_wal(entry);
}

const LogEntry& LogStore::get(uint64_t index) const {
    if (index == 0 || index > entries_.size())
        throw std::out_of_range("Log index out of range");
    return entries_[index - 1];
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
    rewrite_wal();  // Must rewrite since we can't truncate mid-file cleanly
}

// --- MetadataStore ---

MetadataStore::MetadataStore(const std::string& path, bool persist)
    : persist_(persist) {
    if (persist_) {
        std::filesystem::create_directories(path);
        filepath_ = path + "metadata.bin";
    }
}

void MetadataStore::save(uint64_t term, uint32_t voted_for) {
    if (!persist_) return;

    // Write to tmp, then atomic rename
    std::string tmp = filepath_ + ".tmp";
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);

    out.write(reinterpret_cast<const char*>(&term), 8);
    out.write(reinterpret_cast<const char*>(&voted_for), 4);

    // CRC over term + voted_for
    uint8_t buf[12];
    std::memcpy(buf, &term, 8);
    std::memcpy(buf + 8, &voted_for, 4);
    uint32_t checksum = crc32(buf, 12);
    out.write(reinterpret_cast<const char*>(&checksum), 4);

    out.flush();
    out.close();

    std::filesystem::rename(tmp, filepath_);
}

void MetadataStore::load(uint64_t& term, uint32_t& voted_for) {
    term = 0;
    voted_for = 0;
    if (!persist_) return;

    std::ifstream in(filepath_, std::ios::binary);
    if (!in) return;

    in.read(reinterpret_cast<char*>(&term), 8);
    in.read(reinterpret_cast<char*>(&voted_for), 4);
    uint32_t stored_crc = 0;
    in.read(reinterpret_cast<char*>(&stored_crc), 4);

    if (!in) {
        term = 0;
        voted_for = 0;
        return;
    }

    // Verify
    uint8_t buf[12];
    std::memcpy(buf, &term, 8);
    std::memcpy(buf + 8, &voted_for, 4);
    uint32_t computed = crc32(buf, 12);
    if (stored_crc != computed) {
        spdlog::warn("Metadata corruption detected, resetting");
        term = 0;
        voted_for = 0;
    } else {
        spdlog::info("Metadata recovery: term={}, votedFor={}", term, voted_for);
    }
}

}  // namespace raftkv
