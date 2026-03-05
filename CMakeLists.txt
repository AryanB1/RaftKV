cmake_minimum_required(VERSION 3.20)
project(raftkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Dependencies
find_package(Protobuf REQUIRED)
find_package(absl REQUIRED)
find_package(spdlog REQUIRED)
find_package(GTest REQUIRED)

# Generate protobuf sources
set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${PROTO_SRC_DIR})

set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/raft.proto)

add_custom_command(
    OUTPUT ${PROTO_SRC_DIR}/raft.pb.cc ${PROTO_SRC_DIR}/raft.pb.h
    COMMAND protobuf::protoc
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
        --cpp_out=${PROTO_SRC_DIR}
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating protobuf sources"
)

add_library(raft_proto
    ${PROTO_SRC_DIR}/raft.pb.cc
)
target_include_directories(raft_proto PUBLIC ${PROTO_SRC_DIR})
target_link_libraries(raft_proto PUBLIC
    protobuf::libprotobuf
    absl::log
    absl::log_internal_message
    absl::log_internal_check_op
)

# Core library (networking, raft, storage, kv)
add_library(raftkv_core
    src/network/tcp_server.cpp
    src/network/tcp_client.cpp
    src/network/message.cpp
    src/raft/raft_node.cpp
    src/storage/log_store.cpp
    src/kv/state_machine.cpp
)
target_include_directories(raftkv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(raftkv_core PUBLIC raft_proto spdlog::spdlog)

# Node executable
add_executable(raftkv_node src/main.cpp)
target_link_libraries(raftkv_node PRIVATE raftkv_core)

# CLI client executable
add_executable(raftkv_client src/client/client_main.cpp)
target_link_libraries(raftkv_client PRIVATE raftkv_core)

# Tests
enable_testing()
add_executable(raftkv_tests
    tests/network_test.cpp
    tests/raft_test.cpp
    tests/kv_test.cpp
)
target_link_libraries(raftkv_tests PRIVATE raftkv_core GTest::gtest_main)
add_test(NAME raftkv_tests COMMAND raftkv_tests)
