#pragma once

#include <optional>
#include <string>
#include <unordered_map>

namespace raftkv {

// Stub — will be fully implemented in Phase 3
class StateMachine {
public:
    // Apply a command string, return result (value for GET, empty for PUT/DELETE)
    std::string apply(const std::string& command);

    std::optional<std::string> get(const std::string& key) const;

private:
    std::unordered_map<std::string, std::string> store_;
};

}  // namespace raftkv
