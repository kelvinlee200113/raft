#include <gtest/gtest.h>
#include <wal/wal.h>
#include <wal/proto.h>
#include <raft/proto.h>
#include <string>
#include <vector>
#include <cstdio>
#include <cstring>

// ---------------------------------------------------------------------------
// Fixture: creates a unique temp directory per test, removes it on teardown
// ---------------------------------------------------------------------------
class WALTest : public ::testing::Test {
protected:
  std::string dir_;

  void SetUp() override {
    // Each test gets its own directory so tests are fully isolated
    dir_ = std::string("/tmp/wal_test_") + GetCurrentTestName();
    // Clean any leftover from a previous crashed run
    std::string rm_cmd = "rm -rf " + dir_;
    system(rm_cmd.c_str());
  }

  void TearDown() override {
    std::string rm_cmd = "rm -rf " + dir_;
    system(rm_cmd.c_str());
  }

private:
  static const char* GetCurrentTestName() {
    return ::testing::UnitTest::GetInstance()->current_test_info()->name();
  }
};

// ---------------------------------------------------------------------------
// Test 1: Write a HardState, close, reopen, recover — get it back exactly
// ---------------------------------------------------------------------------
TEST_F(WALTest, RecoverHardState) {
  // --- write phase ---
  auto w = kv::wal::WAL::create(dir_);
  ASSERT_NE(w, nullptr);

  kv::wal::HardStateProto hs{/*term=*/5, /*vote=*/2, /*commit=*/3};
  w->save_hard_state(hs);
  ASSERT_TRUE(w->sync());

  // Destroy WAL → closes underlying file
  w.reset();

  // --- recover phase ---
  auto w2 = kv::wal::WAL::open(dir_);
  ASSERT_NE(w2, nullptr);

  std::vector<kv::proto::Entry> entries;
  auto recovered = w2->recover(entries);

  EXPECT_EQ(recovered.term,   5u);
  EXPECT_EQ(recovered.vote,   2u);
  EXPECT_EQ(recovered.commit, 3u);
  EXPECT_TRUE(entries.empty());  // no entries were written
}

// ---------------------------------------------------------------------------
// Test 2: Write multiple entries, close, reopen, recover — get them back
// ---------------------------------------------------------------------------
TEST_F(WALTest, RecoverEntries) {
  auto w = kv::wal::WAL::create(dir_);
  ASSERT_NE(w, nullptr);

  // Write 3 entries
  for (int i = 1; i <= 3; i++) {
    kv::proto::Entry e;
    e.type  = kv::proto::EntryNormal;
    e.term  = 1;
    e.index = static_cast<uint64_t>(i);
    e.data  = {static_cast<uint8_t>('a' + i - 1)};  // 'a', 'b', 'c'
    w->save_entry(e);
  }
  ASSERT_TRUE(w->sync());
  w.reset();

  // --- recover ---
  auto w2 = kv::wal::WAL::open(dir_);
  ASSERT_NE(w2, nullptr);

  std::vector<kv::proto::Entry> entries;
  auto hs = w2->recover(entries);

  // No HardState was written → all zeros
  EXPECT_TRUE(hs.is_empty());

  // All 3 entries recovered in order
  ASSERT_EQ(entries.size(), 3u);
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(entries[i].term,  1u);
    EXPECT_EQ(entries[i].index, static_cast<uint64_t>(i + 1));
    EXPECT_EQ(entries[i].data, (std::vector<uint8_t>{static_cast<uint8_t>('a' + i)}));
  }
}

// ---------------------------------------------------------------------------
// Test 3: Interleaved HardState and entries — recover returns both correctly
// ---------------------------------------------------------------------------
TEST_F(WALTest, RecoverMixedRecords) {
  auto w = kv::wal::WAL::create(dir_);
  ASSERT_NE(w, nullptr);

  // HardState, Entry, Entry, HardState, Entry  (interleaved)
  kv::wal::HardStateProto hs1{1, 1, 0};
  w->save_hard_state(hs1);

  kv::proto::Entry e1; e1.term = 1; e1.index = 1; e1.data = {10};
  kv::proto::Entry e2; e2.term = 1; e2.index = 2; e2.data = {20};
  w->save_entry(e1);
  w->save_entry(e2);

  kv::wal::HardStateProto hs2{2, 2, 1};  // term advanced, commit advanced
  w->save_hard_state(hs2);

  kv::proto::Entry e3; e3.term = 2; e3.index = 3; e3.data = {30};
  w->save_entry(e3);

  ASSERT_TRUE(w->sync());
  w.reset();

  // --- recover ---
  auto w2 = kv::wal::WAL::open(dir_);
  ASSERT_NE(w2, nullptr);

  std::vector<kv::proto::Entry> entries;
  auto hs = w2->recover(entries);

  // Last HardState wins
  EXPECT_EQ(hs.term,   2u);
  EXPECT_EQ(hs.vote,   2u);
  EXPECT_EQ(hs.commit, 1u);

  // All 3 entries in order
  ASSERT_EQ(entries.size(), 3u);
  EXPECT_EQ(entries[0].index, 1u);
  EXPECT_EQ(entries[0].data,  std::vector<uint8_t>{10});
  EXPECT_EQ(entries[1].index, 2u);
  EXPECT_EQ(entries[1].data,  std::vector<uint8_t>{20});
  EXPECT_EQ(entries[2].index, 3u);
  EXPECT_EQ(entries[2].data,  std::vector<uint8_t>{30});
}

// ---------------------------------------------------------------------------
// Test 4: Multiple HardStates → recover returns the LAST one
// ---------------------------------------------------------------------------
TEST_F(WALTest, LastHardStateWins) {
  auto w = kv::wal::WAL::create(dir_);
  ASSERT_NE(w, nullptr);

  w->save_hard_state(kv::wal::HardStateProto{1, 1, 0});
  w->save_hard_state(kv::wal::HardStateProto{3, 2, 1});
  w->save_hard_state(kv::wal::HardStateProto{7, 3, 5});  // ← this one wins
  ASSERT_TRUE(w->sync());
  w.reset();

  auto w2 = kv::wal::WAL::open(dir_);
  ASSERT_NE(w2, nullptr);

  std::vector<kv::proto::Entry> entries;
  auto hs = w2->recover(entries);

  EXPECT_EQ(hs.term,   7u);
  EXPECT_EQ(hs.vote,   3u);
  EXPECT_EQ(hs.commit, 5u);
  EXPECT_TRUE(entries.empty());
}

// ---------------------------------------------------------------------------
// Test 5: Corrupt the payload of the second record → recover returns only
//         the first record (stops at CRC mismatch)
// ---------------------------------------------------------------------------
TEST_F(WALTest, CorruptionStopsRecovery) {
  // Write two entries, sync, close
  auto w = kv::wal::WAL::create(dir_);
  ASSERT_NE(w, nullptr);

  kv::proto::Entry e1; e1.term = 1; e1.index = 1; e1.data = {0xAA, 0xBB};
  kv::proto::Entry e2; e2.term = 1; e2.index = 2; e2.data = {0xCC, 0xDD};
  w->save_entry(e1);
  ASSERT_TRUE(w->sync());   // flush first entry
  w->save_entry(e2);
  ASSERT_TRUE(w->sync());   // flush second entry
  w.reset();                 // close file

  // --- corrupt the .wal file ---
  // The file is: 0000000000000000-0000000000000000.wal
  std::string wal_path = dir_ + "/0000000000000000-0000000000000000.wal";

  // Read the whole file into memory
  FILE* f = fopen(wal_path.c_str(), "rb");
  ASSERT_NE(f, nullptr);
  fseek(f, 0, SEEK_END);
  long file_size = ftell(f);
  ASSERT_GT(file_size, 0);
  fseek(f, 0, SEEK_SET);

  std::vector<uint8_t> raw(static_cast<size_t>(file_size));
  size_t got = fread(raw.data(), 1, raw.size(), f);
  fclose(f);
  ASSERT_EQ(got, raw.size());

  // Record layout: [header:8B][payload:N]
  // First record header at offset 0, payload at offset 8.
  // First record payload length is in bytes 1-3 (little-endian).
  uint32_t first_len = raw[1] | (raw[2] << 8) | (raw[3] << 16);
  // Second record starts right after first record
  size_t second_record_start = 8 + first_len;
  // Second record payload starts 8 bytes after that (skip its header)
  size_t second_payload_start = second_record_start + 8;

  ASSERT_LT(second_payload_start, raw.size());

  // Flip a byte in the second record's payload → CRC will mismatch
  raw[second_payload_start] ^= 0xFF;

  // Write corrupted data back
  f = fopen(wal_path.c_str(), "wb");
  ASSERT_NE(f, nullptr);
  fwrite(raw.data(), 1, raw.size(), f);
  fclose(f);

  // --- recover should get only the first entry ---
  auto w2 = kv::wal::WAL::open(dir_);
  ASSERT_NE(w2, nullptr);

  std::vector<kv::proto::Entry> entries;
  auto hs = w2->recover(entries);

  EXPECT_TRUE(hs.is_empty());          // no HardState written
  ASSERT_EQ(entries.size(), 1u);       // only the first entry survived
  EXPECT_EQ(entries[0].index, 1u);
  EXPECT_EQ(entries[0].data, (std::vector<uint8_t>{0xAA, 0xBB}));
}

// ---------------------------------------------------------------------------
// Test 6: Write a snapshot, close, reopen, recover — get it back exactly
// ---------------------------------------------------------------------------
TEST_F(WALTest, RecoverSnapshot) {
  auto w = kv::wal::WAL::create(dir_);
  ASSERT_NE(w, nullptr);

  kv::wal::SnapshotMeta snap{/*index=*/10, /*term=*/3, /*state=*/{0x01, 0x02, 0x03}};
  w->save_snapshot(snap);
  ASSERT_TRUE(w->sync());
  w.reset();

  // --- recover ---
  auto w2 = kv::wal::WAL::open(dir_);
  ASSERT_NE(w2, nullptr);

  std::vector<kv::proto::Entry> entries;
  kv::wal::SnapshotMeta recovered;
  auto hs = w2->recover(entries, &recovered);

  EXPECT_TRUE(hs.is_empty());
  EXPECT_TRUE(entries.empty());
  EXPECT_EQ(recovered.index, 10u);
  EXPECT_EQ(recovered.term,  3u);
  EXPECT_EQ(recovered.state, (std::vector<uint8_t>{0x01, 0x02, 0x03}));
}

// ---------------------------------------------------------------------------
// Test 7: Multiple snapshots → recover returns the LAST one
// ---------------------------------------------------------------------------
TEST_F(WALTest, LastSnapshotWins) {
  auto w = kv::wal::WAL::create(dir_);
  ASSERT_NE(w, nullptr);

  w->save_snapshot(kv::wal::SnapshotMeta{5,  1, {0xAA}});
  w->save_snapshot(kv::wal::SnapshotMeta{20, 4, {0xBB, 0xCC}});  // ← this one wins
  ASSERT_TRUE(w->sync());
  w.reset();

  auto w2 = kv::wal::WAL::open(dir_);
  ASSERT_NE(w2, nullptr);

  std::vector<kv::proto::Entry> entries;
  kv::wal::SnapshotMeta recovered;
  w2->recover(entries, &recovered);

  EXPECT_EQ(recovered.index, 20u);
  EXPECT_EQ(recovered.term,  4u);
  EXPECT_EQ(recovered.state, (std::vector<uint8_t>{0xBB, 0xCC}));
}

// ---------------------------------------------------------------------------
// Test 8: Realistic recovery: entries before snapshot, snapshot, entries after.
//         All records come back; caller filters entries > snapshot.index.
// ---------------------------------------------------------------------------
TEST_F(WALTest, SnapshotWithEntriesAndHardState) {
  auto w = kv::wal::WAL::create(dir_);
  ASSERT_NE(w, nullptr);

  // Entries 1-3 (will be baked into the snapshot)
  for (int i = 1; i <= 3; i++) {
    kv::proto::Entry e;
    e.term  = 1;
    e.index = static_cast<uint64_t>(i);
    e.data  = {static_cast<uint8_t>(i * 10)};
    w->save_entry(e);
  }

  // Snapshot at index 3 — captures state after entries 1-3
  w->save_snapshot(kv::wal::SnapshotMeta{3, 1, {0xDE, 0xAD}});

  // HardState reflecting commit up to 5
  w->save_hard_state(kv::wal::HardStateProto{2, 1, 5});

  // Entries 4-5 (after the snapshot, need replay)
  for (int i = 4; i <= 5; i++) {
    kv::proto::Entry e;
    e.term  = 2;
    e.index = static_cast<uint64_t>(i);
    e.data  = {static_cast<uint8_t>(i * 10)};
    w->save_entry(e);
  }

  ASSERT_TRUE(w->sync());
  w.reset();

  // --- recover ---
  auto w2 = kv::wal::WAL::open(dir_);
  ASSERT_NE(w2, nullptr);

  std::vector<kv::proto::Entry> entries;
  kv::wal::SnapshotMeta snap;
  auto hs = w2->recover(entries, &snap);

  // HardState
  EXPECT_EQ(hs.term,   2u);
  EXPECT_EQ(hs.commit, 5u);

  // Snapshot
  EXPECT_EQ(snap.index, 3u);
  EXPECT_EQ(snap.term,  1u);
  EXPECT_EQ(snap.state, (std::vector<uint8_t>{0xDE, 0xAD}));

  // All 5 entries come back raw — caller is responsible for skipping <= snap.index
  ASSERT_EQ(entries.size(), 5u);

  // Simulate what main.cpp would do: only replay entries after snapshot
  std::vector<kv::proto::Entry> to_replay;
  for (const auto& e : entries) {
    if (e.index > snap.index) {
      to_replay.push_back(e);
    }
  }
  ASSERT_EQ(to_replay.size(), 2u);
  EXPECT_EQ(to_replay[0].index, 4u);
  EXPECT_EQ(to_replay[0].data,  (std::vector<uint8_t>{40}));
  EXPECT_EQ(to_replay[1].index, 5u);
  EXPECT_EQ(to_replay[1].data,  (std::vector<uint8_t>{50}));
}
