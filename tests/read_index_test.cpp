#include <gtest/gtest.h>
#include <raft/config.h>
#include <raft/proto.h>
#include <raft/raft.h>

class ReadIndexTest : public ::testing::Test {
protected:
  void SetUp() override {
    config.id = 1;
    config.peers = {1, 2, 3};
    config.election_tick = 10;
    config.heartbeat_tick = 1;
  }

  kv::Config config;
};

// Test: ReadIndex returns 0 for non-leaders
TEST_F(ReadIndexTest, NonLeaderReturnsZero) {
  kv::Raft raft(config);

  // Follower tries to serve read
  uint64_t read_idx = raft.read_index();

  EXPECT_EQ(read_idx, 0);
}

// Test: Leader can initiate ReadIndex
TEST_F(ReadIndexTest, LeaderInitiatesReadIndex) {
  kv::Raft raft(config);

  // Become leader
  raft.become_candidate();
  raft.campaign();
  raft.read_messages(); // Clear campaign messages

  // Win election
  kv::proto::Message vote_response;
  vote_response.type = kv::proto::MsgRequestVoteResponse;
  vote_response.from = 2;
  vote_response.to = 1;
  vote_response.term = 1;
  vote_response.vote_granted = true;
  raft.handle_request_vote_response(vote_response);

  ASSERT_EQ(raft.get_state(), kv::State::Leader);

  // Clear initial heartbeat from become_leader()
  raft.read_messages();

  // Initiate ReadIndex
  uint64_t read_idx = raft.read_index();

  // Should return current commit index (0 since no entries)
  EXPECT_EQ(read_idx, 0);

  // Should have sent heartbeats
  auto msgs = raft.read_messages();
  EXPECT_EQ(msgs.size(), 2); // To peers 2 and 3

  for (const auto &msg : msgs) {
    EXPECT_EQ(msg.type, kv::proto::MsgAppendEntries);
    EXPECT_TRUE(msg.entries.empty()); // Heartbeat
  }
}

// Test: ReadIndex becomes ready after majority responds
TEST_F(ReadIndexTest, ReadIndexBecomesReadyAfterMajority) {
  kv::Raft raft(config);

  // Become leader
  raft.become_candidate();
  raft.campaign();
  raft.read_messages();

  kv::proto::Message vote_response;
  vote_response.type = kv::proto::MsgRequestVoteResponse;
  vote_response.from = 2;
  vote_response.to = 1;
  vote_response.term = 1;
  vote_response.vote_granted = true;
  raft.handle_request_vote_response(vote_response);

  ASSERT_EQ(raft.get_state(), kv::State::Leader);
  raft.read_messages(); // Clear initial heartbeat

  // Initiate ReadIndex
  uint64_t read_idx = raft.read_index();
  raft.read_messages(); // Clear heartbeat messages

  // Not ready yet (no responses)
  EXPECT_FALSE(raft.read_index_ready(read_idx));

  // Simulate heartbeat response from peer 2
  kv::proto::Message hb_response;
  hb_response.type = kv::proto::MsgAppendEntriesResponse;
  hb_response.from = 2;
  hb_response.to = 1;
  hb_response.term = 1;
  hb_response.success = true;
  hb_response.match_index = 0;
  raft.handle_append_entries_response(hb_response);

  // Now ready! (Leader=1 + Peer2=1 = 2/3 majority)
  EXPECT_TRUE(raft.read_index_ready(read_idx));
}

// Test: ReadIndex with committed entries
TEST_F(ReadIndexTest, ReadIndexWithCommittedEntries) {
  kv::Raft raft(config);

  // Become leader
  raft.become_candidate();
  raft.campaign();
  raft.read_messages();

  kv::proto::Message vote_response;
  vote_response.type = kv::proto::MsgRequestVoteResponse;
  vote_response.from = 2;
  vote_response.to = 1;
  vote_response.term = 1;
  vote_response.vote_granted = true;
  raft.handle_request_vote_response(vote_response);

  ASSERT_EQ(raft.get_state(), kv::State::Leader);
  raft.read_messages();

  // Propose an entry
  std::vector<uint8_t> data = {1, 2, 3};
  raft.propose(data);

  // Simulate replication to peer 2 (majority)
  kv::proto::Message rep_response;
  rep_response.type = kv::proto::MsgAppendEntriesResponse;
  rep_response.from = 2;
  rep_response.to = 1;
  rep_response.term = 1;
  rep_response.success = true;
  rep_response.match_index = 1; // Replicated entry 1
  raft.handle_append_entries_response(rep_response);

  // Entry should be committed now
  EXPECT_EQ(raft.get_commit_index(), 1);

  raft.read_messages(); // Clear replication messages

  // Now do ReadIndex
  uint64_t read_idx = raft.read_index();

  // Should return commit index = 1
  EXPECT_EQ(read_idx, 1);

  raft.read_messages(); // Clear heartbeat

  // Simulate heartbeat response
  kv::proto::Message hb_response;
  hb_response.type = kv::proto::MsgAppendEntriesResponse;
  hb_response.from = 2;
  hb_response.to = 1;
  hb_response.term = 1;
  hb_response.success = true;
  hb_response.match_index = 1;
  raft.handle_append_entries_response(hb_response);

  // ReadIndex should be ready
  EXPECT_TRUE(raft.read_index_ready(read_idx));
}

// Test: Multiple ReadIndex requests
TEST_F(ReadIndexTest, MultipleReadIndexRequests) {
  kv::Raft raft(config);

  // Become leader
  raft.become_candidate();
  raft.campaign();
  raft.read_messages();

  kv::proto::Message vote_response;
  vote_response.type = kv::proto::MsgRequestVoteResponse;
  vote_response.from = 2;
  vote_response.to = 1;
  vote_response.term = 1;
  vote_response.vote_granted = true;
  raft.handle_request_vote_response(vote_response);

  ASSERT_EQ(raft.get_state(), kv::State::Leader);
  raft.read_messages();

  // First ReadIndex request
  uint64_t read_idx1 = raft.read_index();
  raft.read_messages();

  // Get heartbeat response for first request
  kv::proto::Message hb_response;
  hb_response.type = kv::proto::MsgAppendEntriesResponse;
  hb_response.from = 2;
  hb_response.to = 1;
  hb_response.term = 1;
  hb_response.success = true;
  hb_response.match_index = 0;
  raft.handle_append_entries_response(hb_response);

  // First request should be ready
  EXPECT_TRUE(raft.read_index_ready(read_idx1));

  // Second ReadIndex request (clears previous acks)
  uint64_t read_idx2 = raft.read_index();
  raft.read_messages();

  // Not ready yet (no new responses)
  EXPECT_FALSE(raft.read_index_ready(read_idx2));

  // Get heartbeat response for second request
  raft.handle_append_entries_response(hb_response);

  // Second request should now be ready
  EXPECT_TRUE(raft.read_index_ready(read_idx2));
}
