#pragma once

#include "raft.pb.h"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace raftkv {

// Wire format: [4-byte big-endian length][serialized protobuf payload]
// This handles framing for TCP streams.

class MessageFramer {
public:
    // Serialize a RaftMessage into a framed wire buffer (length prefix + payload)
    static std::vector<uint8_t> frame(const RaftMessage& msg);

    // Feed raw bytes from TCP into the framer.
    // Returns a complete RaftMessage when one is ready, std::nullopt otherwise.
    // Call repeatedly after feeding data — there may be multiple messages buffered.
    void feed(const uint8_t* data, size_t len);
    std::optional<RaftMessage> poll();

private:
    std::vector<uint8_t> buffer_;
};

}  // namespace raftkv
