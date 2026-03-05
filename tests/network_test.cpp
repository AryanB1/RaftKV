#include "network/message.h"
#include "network/tcp_client.h"
#include "network/tcp_server.h"
#include <gtest/gtest.h>
#include <chrono>
#include <thread>

using namespace raftkv;

TEST(MessageFramerTest, FrameAndParse) {
    RaftMessage original;
    original.set_type(RaftMessage::APPEND_ENTRIES_REQ);
    original.set_sender_id(42);
    auto* ae = original.mutable_append_entries_req();
    ae->set_term(5);
    ae->set_leader_id(42);
    ae->set_prev_log_index(10);
    ae->set_prev_log_term(4);
    ae->set_leader_commit(8);

    auto framed = MessageFramer::frame(original);
    ASSERT_GT(framed.size(), 4u);

    MessageFramer framer;
    framer.feed(framed.data(), framed.size());
    auto parsed = framer.poll();
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->type(), RaftMessage::APPEND_ENTRIES_REQ);
    EXPECT_EQ(parsed->sender_id(), 42u);
    EXPECT_EQ(parsed->append_entries_req().term(), 5u);
    EXPECT_EQ(parsed->append_entries_req().leader_commit(), 8u);
}

TEST(MessageFramerTest, PartialFeeds) {
    RaftMessage original;
    original.set_type(RaftMessage::REQUEST_VOTE_REQ);
    original.set_sender_id(1);
    auto* rv = original.mutable_request_vote_req();
    rv->set_term(3);
    rv->set_candidate_id(1);

    auto framed = MessageFramer::frame(original);

    MessageFramer framer;
    // Feed one byte at a time
    for (size_t i = 0; i < framed.size(); i++) {
        framer.feed(&framed[i], 1);
        auto msg = framer.poll();
        if (i < framed.size() - 1) {
            EXPECT_FALSE(msg.has_value());
        } else {
            ASSERT_TRUE(msg.has_value());
            EXPECT_EQ(msg->sender_id(), 1u);
        }
    }
}

TEST(MessageFramerTest, MultipleMessages) {
    MessageFramer framer;

    for (int i = 0; i < 5; i++) {
        RaftMessage msg;
        msg.set_type(RaftMessage::APPEND_ENTRIES_REQ);
        msg.set_sender_id(i);
        auto buf = MessageFramer::frame(msg);
        framer.feed(buf.data(), buf.size());
    }

    for (int i = 0; i < 5; i++) {
        auto msg = framer.poll();
        ASSERT_TRUE(msg.has_value());
        EXPECT_EQ(msg->sender_id(), static_cast<uint32_t>(i));
    }
    EXPECT_FALSE(framer.poll().has_value());
}

TEST(TcpTest, ServerClientExchange) {
    RaftMessage received_msg;
    bool message_received = false;

    TcpServer server(0, // 0 = let OS pick a port
        [&](int fd, const RaftMessage& msg) {
            received_msg = msg;
            message_received = true;

            // Echo back
            RaftMessage resp;
            resp.set_type(RaftMessage::APPEND_ENTRIES_RESP);
            resp.set_sender_id(99);
            auto* ae_resp = resp.mutable_append_entries_resp();
            ae_resp->set_term(msg.append_entries_req().term());
            ae_resp->set_success(true);
            server.send(fd, resp);
        });

    // We need the actual port; use a known port for the test
    uint16_t test_port = 19876;
    TcpServer server2(test_port,
        [&](int fd, const RaftMessage& msg) {
            received_msg = msg;
            message_received = true;

            RaftMessage resp;
            resp.set_type(RaftMessage::APPEND_ENTRIES_RESP);
            resp.set_sender_id(99);
            auto* ae_resp = resp.mutable_append_entries_resp();
            ae_resp->set_term(msg.append_entries_req().term());
            ae_resp->set_success(true);
            server2.send(fd, resp);
        });
    server2.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    RaftMessage client_received;
    bool client_got_response = false;

    TcpClient client("127.0.0.1", test_port,
        [&](const RaftMessage& msg) {
            client_received = msg;
            client_got_response = true;
        });

    ASSERT_TRUE(client.connect());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Send a message
    RaftMessage ping;
    ping.set_type(RaftMessage::APPEND_ENTRIES_REQ);
    ping.set_sender_id(7);
    auto* ae = ping.mutable_append_entries_req();
    ae->set_term(1);
    ae->set_leader_id(7);

    ASSERT_TRUE(client.send(ping));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify server received message
    EXPECT_TRUE(message_received);
    EXPECT_EQ(received_msg.sender_id(), 7u);
    EXPECT_EQ(received_msg.append_entries_req().term(), 1u);

    // Verify client received response
    EXPECT_TRUE(client_got_response);
    EXPECT_EQ(client_received.type(), RaftMessage::APPEND_ENTRIES_RESP);
    EXPECT_EQ(client_received.sender_id(), 99u);

    client.disconnect();
    server2.stop();
}
