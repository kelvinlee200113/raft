cmake_minimum_required(VERSION 3.14)
project(raft-kv)

# Generate compile_commands.json for language servers (clangd, etc.)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Use C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
include_directories(src)

# Create raft library (compile once, link many times)
add_library(raft_lib STATIC
    src/raft/raft.cpp
)

# Add the main executable
add_executable(raft-kv
    src/main.cpp
)
target_link_libraries(raft-kv raft_lib)

# Find Google Test
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Enable testing
enable_testing()

# Add tests
add_executable(voting_test tests/voting_test.cpp)
target_link_libraries(voting_test raft_lib GTest::gtest GTest::gtest_main)
add_test(NAME VotingTest COMMAND voting_test)

add_executable(replication_test tests/replication_test.cpp)
target_link_libraries(replication_test raft_lib GTest::gtest GTest::gtest_main)
add_test(NAME ReplicationTest COMMAND replication_test)


