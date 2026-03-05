#include "kv/state_machine.h"
#include <algorithm>
#include <sstream>

namespace raftkv {

std::string StateMachine::apply(const std::string& command) {
    std::istringstream ss(command);
    std::string op;
    ss >> op;

    if (op == "PUT") {
        std::string key, value;
        ss >> key;
        // Value is the rest of the line (supports spaces in values)
        std::getline(ss >> std::ws, value);
        if (key.empty()) return "ERR PUT requires key and value";
        if (value.empty()) return "ERR PUT requires a value";
        store_[key] = value;
        return "OK";
    } else if (op == "GET") {
        std::string key;
        ss >> key;
        if (key.empty()) return "ERR GET requires a key";
        auto it = store_.find(key);
        if (it != store_.end()) return it->second;
        return "(nil)";
    } else if (op == "DELETE") {
        std::string key;
        ss >> key;
        if (key.empty()) return "ERR DELETE requires a key";
        if (store_.erase(key) > 0) return "OK";
        return "(nil)";
    } else if (op == "LIST") {
        if (store_.empty()) return "(empty)";
        std::vector<std::string> sorted_keys;
        sorted_keys.reserve(store_.size());
        for (auto& [k, v] : store_) sorted_keys.push_back(k);
        std::sort(sorted_keys.begin(), sorted_keys.end());
        std::string result;
        for (size_t i = 0; i < sorted_keys.size(); i++) {
            if (i > 0) result += "\n";
            result += sorted_keys[i];
        }
        return result;
    }

    return "ERR unknown command '" + op + "'";
}

std::optional<std::string> StateMachine::get(const std::string& key) const {
    auto it = store_.find(key);
    if (it != store_.end()) return it->second;
    return std::nullopt;
}

std::vector<std::string> StateMachine::keys() const {
    std::vector<std::string> result;
    result.reserve(store_.size());
    for (auto& [k, v] : store_) result.push_back(k);
    return result;
}

}  // namespace raftkv
