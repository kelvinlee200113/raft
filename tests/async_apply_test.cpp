#include <gtest/gtest.h>
#include <raft/raft.h>
#include <server/kv_store.h>
#include <thread>
#include <chrono>
#include <atomic>

using namespace kv;

// Test that entries are applied asynchronously after commit
TEST(AsyncApplyTest, EntriesAppliedAfterCommit) {
  Config config;
  config.id = 1;
  config.peers = {1, 2, 3};

  Raft raft(config);
  KVStore kv_store;

  // Make this node leader
  raft.become_leader();

  // Propose an entry
  proto::Entry entry;
  entry.type = proto::EntryNormal;
  entry.index = 1;
  entry.term = raft.get_term();
  entry.data = {1, 2, 3, 4, 5};

  raft.test_append_log_entry(entry);

  // Initially, nothing committed or applied
  EXPECT_EQ(raft.get_commit_index(), 0);
  EXPECT_EQ(raft.get_last_applied(), 0);

  // Manually advance commit (simulating majority replication)
  // In real system, this happens via AppendEntries responses
  raft.test_set_commit_index(1);

  // Get entries to apply
  auto entries = raft.get_entries_to_apply();
  EXPECT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].index, 1);

  // Advance apply index (skip actual KVStore apply for this test)
  raft.advance(entries[0].index);

  // Verify applied
  EXPECT_EQ(raft.get_last_applied(), 1);
}

// Test that apply is thread-safe with multiple entries
TEST(AsyncApplyTest, ThreadSafeApplyMultipleEntries) {
  Config config;
  config.id = 1;
  config.peers = {1, 2, 3};

  Raft raft(config);
  KVStore kv_store;

  raft.become_leader();

  // Propose 100 entries
  for (int i = 0; i < 100; i++) {
    proto::Entry entry;
    entry.type = proto::EntryNormal;
    entry.index = i + 1;
    entry.term = raft.get_term();
    entry.data = {static_cast<uint8_t>(i)};
    raft.test_append_log_entry(entry);
  }

  // Simulate commits
  raft.test_set_commit_index(100);

  // Apply in separate thread
  std::atomic<bool> running{true};
  std::thread apply_thread([&]() {
    while (running.load()) {
      auto entries = raft.get_entries_to_apply();
      if (entries.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }

      for (const auto& entry : entries) {
        // Skip actual KVStore apply for this test
        raft.advance(entry.index);
      }
    }
  });

  // Wait for apply to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  running.store(false);
  apply_thread.join();

  // Verify all applied
  EXPECT_EQ(raft.get_last_applied(), 100);
}

// Test that apply doesn't block Raft operations
TEST(AsyncApplyTest, ApplyDoesNotBlockRaft) {
  Config config;
  config.id = 1;
  config.peers = {1, 2, 3};

  Raft raft(config);
  KVStore kv_store;

  raft.become_leader();

  // Propose entry
  proto::Entry entry;
  entry.type = proto::EntryNormal;
  entry.index = 1;
  entry.term = raft.get_term();
  entry.data = {1, 2, 3};
  raft.test_append_log_entry(entry);
  raft.test_set_commit_index(1);

  // Start apply thread (but don't call advance immediately)
  std::atomic<bool> applying{true};
  std::thread apply_thread([&]() {
    auto entries = raft.get_entries_to_apply();
    EXPECT_EQ(entries.size(), 1);

    // Simulate slow state machine (100ms)
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Advance (skip actual KVStore apply for this test)
    for (const auto& e : entries) {
      raft.advance(e.index);
    }

    applying.store(false);
  });

  // While apply is happening, Raft should still work
  raft.tick();  // Should not block

  // Propose another entry
  proto::Entry entry2;
  entry2.type = proto::EntryNormal;
  entry2.index = 2;
  entry2.term = raft.get_term();
  entry2.data = {4, 5, 6};
  raft.test_append_log_entry(entry2);

  // Wait for apply thread
  apply_thread.join();

  // Raft should still be functional
  EXPECT_EQ(raft.get_state(), State::Leader);
  EXPECT_EQ(raft.test_get_log_size(), 2);
  EXPECT_EQ(raft.get_last_applied(), 1);
}

// Test concurrent reads and apply
TEST(AsyncApplyTest, ConcurrentReadsAndApply) {
  Config config;
  config.id = 1;
  config.peers = {1, 2, 3};

  Raft raft(config);
  KVStore kv_store;

  raft.become_leader();

  // Add 50 entries
  for (int i = 0; i < 50; i++) {
    proto::Entry entry;
    entry.type = proto::EntryNormal;
    entry.index = i + 1;
    entry.term = raft.get_term();
    entry.data = {static_cast<uint8_t>(i)};
    raft.test_append_log_entry(entry);
  }
  raft.test_set_commit_index(50);

  std::atomic<bool> running{true};

  // Apply thread
  std::thread apply_thread([&]() {
    while (running.load()) {
      auto entries = raft.get_entries_to_apply();
      for (const auto& entry : entries) {
        // Skip actual KVStore apply for this test
        raft.advance(entry.index);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });

  // Reader thread - continuously checks apply progress
  std::atomic<uint64_t> max_applied{0};
  std::thread reader_thread([&]() {
    while (running.load()) {
      uint64_t applied = raft.get_last_applied();
      if (applied > max_applied.load()) {
        max_applied.store(applied);
      }
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  // Wait for all entries to be applied
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  running.store(false);

  apply_thread.join();
  reader_thread.join();

  // Verify all applied
  EXPECT_EQ(raft.get_last_applied(), 50);
  EXPECT_EQ(max_applied.load(), 50);
}

// Test that advance() is idempotent
TEST(AsyncApplyTest, AdvanceIsIdempotent) {
  Config config;
  config.id = 1;
  config.peers = {1, 2, 3};

  Raft raft(config);

  raft.become_leader();

  // Add entries
  for (int i = 0; i < 10; i++) {
    proto::Entry entry;
    entry.type = proto::EntryNormal;
    entry.index = i + 1;
    entry.term = raft.get_term();
    entry.data = {static_cast<uint8_t>(i)};
    raft.test_append_log_entry(entry);
  }
  raft.test_set_commit_index(10);

  // Advance to 5
  raft.advance(5);
  EXPECT_EQ(raft.get_last_applied(), 5);

  // Try advancing to 3 (should be no-op, can't go backwards)
  raft.advance(3);
  EXPECT_EQ(raft.get_last_applied(), 5);

  // Advance to 7
  raft.advance(7);
  EXPECT_EQ(raft.get_last_applied(), 7);

  // Try advancing to 100 (should be no-op, beyond commit_index)
  raft.advance(100);
  EXPECT_EQ(raft.get_last_applied(), 7);

  // Advance to 10 (should work)
  raft.advance(10);
  EXPECT_EQ(raft.get_last_applied(), 10);
}

// Test get_entries_to_apply() returns correct range
TEST(AsyncApplyTest, GetEntriesToApplyReturnsCorrectRange) {
  Config config;
  config.id = 1;
  config.peers = {1, 2, 3};

  Raft raft(config);

  raft.become_leader();

  // Add 10 entries
  for (int i = 0; i < 10; i++) {
    proto::Entry entry;
    entry.type = proto::EntryNormal;
    entry.index = i + 1;
    entry.term = raft.get_term();
    entry.data = {static_cast<uint8_t>(i)};
    raft.test_append_log_entry(entry);
  }

  // Commit first 5
  raft.test_set_commit_index(5);

  // Get entries to apply (should return 1-5)
  auto entries = raft.get_entries_to_apply();
  EXPECT_EQ(entries.size(), 5);
  for (int i = 0; i < 5; i++) {
    EXPECT_EQ(entries[i].index, i + 1);
  }

  // Advance to 3
  raft.advance(3);

  // Get entries to apply (should return 4-5)
  entries = raft.get_entries_to_apply();
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].index, 4);
  EXPECT_EQ(entries[1].index, 5);

  // Advance to 5
  raft.advance(5);

  // Get entries to apply (should return empty)
  entries = raft.get_entries_to_apply();
  EXPECT_EQ(entries.size(), 0);

  // Commit more (6-10)
  raft.test_set_commit_index(10);

  // Get entries to apply (should return 6-10)
  entries = raft.get_entries_to_apply();
  EXPECT_EQ(entries.size(), 5);
  for (int i = 0; i < 5; i++) {
    EXPECT_EQ(entries[i].index, 6 + i);
  }
}
