#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace raftkv {

class StateMachine {
public:
    std::string apply(const std::string& command);
    std::optional<std::string> get(const std::string& key) const;
    std::vector<std::string> keys() const;
    size_t size() const { return store_.size(); }

private:
    std::unordered_map<std::string, std::string> store_;
};

}  // namespace raftkv
