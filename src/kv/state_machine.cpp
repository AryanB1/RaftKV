#include "kv/state_machine.h"
#include <sstream>

namespace raftkv {

std::string StateMachine::apply(const std::string& command) {
    std::istringstream ss(command);
    std::string op;
    ss >> op;

    if (op == "PUT") {
        std::string key, value;
        ss >> key >> value;
        store_[key] = value;
        return "OK";
    } else if (op == "GET") {
        std::string key;
        ss >> key;
        auto it = store_.find(key);
        if (it != store_.end()) return it->second;
        return "(nil)";
    } else if (op == "DELETE") {
        std::string key;
        ss >> key;
        store_.erase(key);
        return "OK";
    }

    return "ERR unknown command";
}

std::optional<std::string> StateMachine::get(const std::string& key) const {
    auto it = store_.find(key);
    if (it != store_.end()) return it->second;
    return std::nullopt;
}

}  // namespace raftkv
