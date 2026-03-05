#include "network/message.h"
#include <arpa/inet.h>
#include <cstring>

namespace raftkv {

std::vector<uint8_t> MessageFramer::frame(const RaftMessage& msg) {
    std::string serialized = msg.SerializeAsString();
    uint32_t len = htonl(static_cast<uint32_t>(serialized.size()));

    std::vector<uint8_t> buf(4 + serialized.size());
    std::memcpy(buf.data(), &len, 4);
    std::memcpy(buf.data() + 4, serialized.data(), serialized.size());
    return buf;
}

void MessageFramer::feed(const uint8_t* data, size_t len) {
    buffer_.insert(buffer_.end(), data, data + len);
}

std::optional<RaftMessage> MessageFramer::poll() {
    if (buffer_.size() < 4) return std::nullopt;

    uint32_t msg_len;
    std::memcpy(&msg_len, buffer_.data(), 4);
    msg_len = ntohl(msg_len);

    if (buffer_.size() < 4 + msg_len) return std::nullopt;

    RaftMessage msg;
    if (!msg.ParseFromArray(buffer_.data() + 4, msg_len)) {
        // Corrupt message — skip it
        buffer_.erase(buffer_.begin(), buffer_.begin() + 4 + msg_len);
        return std::nullopt;
    }

    buffer_.erase(buffer_.begin(), buffer_.begin() + 4 + msg_len);
    return msg;
}

}  // namespace raftkv
