#include "network/tcp_client.h"
#include <spdlog/spdlog.h>
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <host> <port>\n";
        return 1;
    }

    std::string host = argv[1];
    uint16_t port = std::stoul(argv[2]);

    spdlog::info("Connecting to {}:{}", host, port);

    raftkv::TcpClient client(host, port,
        [](const raftkv::RaftMessage& msg) {
            if (msg.has_client_resp()) {
                auto& resp = msg.client_resp();
                if (resp.success()) {
                    if (!resp.value().empty())
                        std::cout << resp.value() << std::endl;
                    else
                        std::cout << "OK" << std::endl;
                } else {
                    std::cerr << "ERR: " << resp.error() << std::endl;
                }
            }
        });

    if (!client.connect()) {
        spdlog::error("Failed to connect");
        return 1;
    }

    // REPL
    std::string line;
    std::cout << "raftkv> " << std::flush;
    while (std::getline(std::cin, line)) {
        if (line.empty()) {
            std::cout << "raftkv> " << std::flush;
            continue;
        }
        if (line == "quit" || line == "exit") break;

        raftkv::RaftMessage msg;
        msg.set_type(raftkv::RaftMessage::CLIENT_REQ);
        msg.set_sender_id(0);
        auto* req = msg.mutable_client_req();
        req->set_command(line);

        if (!client.send(msg)) {
            std::cerr << "Failed to send command" << std::endl;
        }

        // Give server time to respond
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::cout << "raftkv> " << std::flush;
    }

    return 0;
}
