#include "raft/proto.h"
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <raft/raft.h>

namespace kv {

Raft::Raft(const Config &config)
    : id_(config.id), term_(0), lead_(0), voted_for_(0), commit_index_(0),
      last_applied_(0), state_(State::Follower), log_offset_(0), peers_(config.peers),
      election_timeout_(config.election_tick),
      heartbeat_timeout_(config.heartbeat_tick),
      snapshot_threshold_(config.snapshot_threshold), last_snapshot_index_(0),
      election_elapsed_(0),
      randomized_election_timeout_(0), heartbeat_elapsed_(0),
      read_index_pending_(false), pending_read_index_(0),
      apply_queue_(std::make_unique<LockFreeQueue<proto::Entry>>(10000)) {

  // Initialize atomic (can't be in initializer list)
  apply_thread_running_.store(false);

  // Generate random election timeout (between election_timeout to
  // 2*election_timeout)
  randomized_election_timeout_ =
      election_timeout_ + (rand() % election_timeout_);
}

void Raft::restore(const wal::HardStateProto& hard_state, const std::vector<proto::Entry>& entries) {
  log_ = entries;
  term_ = hard_state.term;
  voted_for_ = hard_state.vote;
  commit_index_ = hard_state.commit;
  // last_applied_ intentionally stays 0 — caller replays committed entries
  // into the state machine and then calls advance(commit_index).
}

void Raft::become_follower(uint64_t term, uint64_t leader) {
  state_ = State::Follower;
  term_ = term;
  lead_ = leader;
  voted_for_ = 0;
  reset_randomized_election_timeout();
}

void Raft::become_pre_candidate() {
  state_ = State::PreCandidate;
  // Don't increment term in PreVote!
  lead_ = 0;
  pre_votes_.clear();
  reset_randomized_election_timeout();
}

void Raft::become_candidate() {
  state_ = State::Candidate;
  term_++;
  lead_ = 0;
  voted_for_ = id_;
  votes_.clear();
  reset_randomized_election_timeout();

  // WAL-first: persist new term + self-vote before campaign() sends messages
  if (wal_) {
    wal_->save_hard_state({term_, voted_for_, commit_index_});
    wal_->sync();
  }
}

void Raft::become_leader() {
  state_ = State::Leader;
  lead_ = id_;
  heartbeat_elapsed_ = 0; // Reset heartbeat timer

  // Remove stale data
  progress_.clear();

  for (uint64_t peer_id : peers_) {
    if (peer_id == id_) {
      continue;
    }
    Progress progress;
    progress.next = last_log_index() + 1;
    progress.match = 0;
    progress_[peer_id] = progress;
  }

  // Send initial heartbeat immediately
  broadcast_heartbeat();
}

void Raft::tick() {
  if (state_ == State::Leader) {
    // Leader sends periodic heartbeats
    heartbeat_elapsed_++;
    if (heartbeat_elapsed_ >= heartbeat_timeout_) {
      heartbeat_elapsed_ = 0;
      broadcast_heartbeat();
    }
  } else {
    // Followers and candidates track election timeout
    election_elapsed_++;

    // Check if election timeout has passed
    if (election_elapsed_ >= randomized_election_timeout_) {
      election_elapsed_ = 0;

      // Followers start with PreVote, PreCandidates and Candidates re-campaign
      if (state_ == State::Follower) {
        become_pre_candidate();
        pre_campaign(); // Send PreVote messages to all peers
      } else if (state_ == State::PreCandidate) {
        // PreVote failed, retry
        become_pre_candidate();
        pre_campaign();
      } else if (state_ == State::Candidate) {
        // Real vote failed, go back to PreVote
        become_pre_candidate();
        pre_campaign();
      }
    }
  }
}

void Raft::reset_randomized_election_timeout() {
  election_elapsed_ = 0;
  randomized_election_timeout_ =
      election_timeout_ + (rand() % election_timeout_);
}

proto::Message Raft::handle_request_vote(const proto::Message &msg) {
  proto::Message response;
  response.type = proto::MsgRequestVoteResponse;
  response.from = id_;
  response.to = msg.from;
  response.vote_granted = false;

  // Update term if candidate's term is higher
  if (msg.term > term_) {
    become_follower(msg.term, 0);
  }

  // Check if should grant vote
  if (msg.term >= term_ && (voted_for_ == 0 || voted_for_ == msg.from)) {
    response.vote_granted = true;
    voted_for_ = msg.from;
    reset_randomized_election_timeout();

    // WAL-first: persist vote before sending response
    if (wal_) {
      wal_->save_hard_state({term_, voted_for_, commit_index_});
      wal_->sync();
    }
  }

  // Set response term
  response.term = term_;

  return response;
}

void Raft::handle_request_vote_response(const proto::Message &msg) {
  // Ensure we are still the candidate
  if (state_ != State::Candidate) {
    return;
  }

  // Check term
  if (msg.term < term_) {
    // Stale
    return;
  }

  // If response is from higher term, step down
  if (msg.term > term_) {
    become_follower(msg.term, 0);
    return;
  }

  // Record granted vote
  if (msg.vote_granted) {
    votes_[msg.from] = true;
  }

  // Count total votes
  uint64_t total_votes = 0;
  for (const auto &vote : votes_) {
    if (vote.second) {
      total_votes++;
    }
  }

  // If majority, become leader
  if (total_votes > peers_.size() / 2) {
    become_leader();
  }
}

// Handle PreVote request (like RequestVote but doesn't increment term)
proto::Message Raft::handle_pre_vote(const proto::Message &msg) {
  proto::Message response;
  response.type = proto::MsgPreVoteResponse;
  response.from = id_;
  response.to = msg.from;
  response.vote_granted = false;

  // Reject if we have a leader and it's still sending heartbeats
  // (election_elapsed_ is low)
  if (lead_ != 0 && election_elapsed_ < election_timeout_) {
    response.term = term_;
    return response; // vote_granted = false
  }

  // Grant pre-vote if candidate's log is at least as up-to-date as ours
  // Same logic as RequestVote
  bool log_ok = false;
  if (log_.empty()) {
    log_ok = true; // We have no log, anyone is ok
  } else {
    uint64_t our_last_index = last_log_index();
    uint64_t our_last_term = log_entry(our_last_index).term;

    if (msg.last_log_term > our_last_term) {
      log_ok = true; // Candidate's last term is newer
    } else if (msg.last_log_term == our_last_term &&
               msg.last_log_index >= our_last_index) {
      log_ok = true; // Same term, candidate's log is at least as long
    }
  }

  if (log_ok) {
    response.vote_granted = true;
  }

  response.term = term_;
  return response;
}

// Handle PreVote response
void Raft::handle_pre_vote_response(const proto::Message &msg) {
  // Ensure we are still pre-candidate
  if (state_ != State::PreCandidate) {
    return;
  }

  // Record granted pre-vote
  if (msg.vote_granted) {
    pre_votes_[msg.from] = true;
  }

  // Count total pre-votes
  uint64_t total_pre_votes = 0;
  for (const auto &vote : pre_votes_) {
    if (vote.second) {
      total_pre_votes++;
    }
  }

  // If majority, transition to real candidate and start real election
  if (total_pre_votes > peers_.size() / 2) {
    become_candidate();
    campaign();
  }
}

// PreCandidate pre-campaigning for itself
void Raft::pre_campaign() {
  // Record pre-vote for self
  pre_votes_[id_] = true;
  for (uint64_t peer_id : peers_) {
    if (peer_id != id_) {
      // Create PreVote message (using CURRENT term, not term+1!)
      proto::Message msg;
      msg.type = proto::MsgPreVote;
      msg.from = id_;
      msg.to = peer_id;
      msg.term = term_; // Current term, not incremented!
      if (log_.empty()) {
        msg.last_log_index = 0;
        msg.last_log_term = 0;
      } else {
        msg.last_log_index = log_.back().index;
        msg.last_log_term = log_.back().term;
      }
      msgs_.push_back(msg);
    }
  }
}

// Candidate campaigning for itself
void Raft::campaign() {
  // Record vote for self
  votes_[id_] = true;
  for (uint64_t peer_id : peers_) {
    if (peer_id != id_) {
      // Create RequestVote message
      proto::Message msg;
      msg.type = proto::MsgRequestVote;
      msg.from = id_;
      msg.to = peer_id;
      msg.term = term_;
      if (log_.empty()) {
        msg.last_log_index = 0;
        msg.last_log_term = 0;
      } else {
        msg.last_log_index = log_.back().index;
        msg.last_log_term = log_.back().term;
      }
      msgs_.push_back(msg);
    }
  }
}

void Raft::send(proto::Message msg) { msgs_.push_back(msg); }

std::vector<proto::Message> Raft::read_messages() {
  std::vector<proto::Message> msgs;
  msgs.swap(msgs_);
  return msgs;
}

// Broadcast AppendEntries message (empty for heartbeats)
void Raft::broadcast_heartbeat() {
  for (uint64_t peer_id : peers_) {
    if (peer_id != id_) {
      uint64_t next_index = progress_[peer_id].next;

      // Check if this peer needs a snapshot (next index is already compacted)
      if (next_index <= log_offset_) {
        // Send InstallSnapshot instead of AppendEntries
        proto::Message snap_msg;
        snap_msg.type = proto::MsgInstallSnapshot;
        snap_msg.from = id_;
        snap_msg.to = peer_id;
        snap_msg.term = term_;
        snap_msg.snapshot_index = log_offset_;
        snap_msg.snapshot_term = (log_offset_ > 0 && !log_.empty()) ? log_.front().term : 0;
        snap_msg.snapshot_data = last_snapshot_data_;  // Send cached snapshot

        msgs_.push_back(snap_msg);
        continue;
      }

      // Send AppendEntries message
      proto::Message msg;
      msg.type = proto::MsgAppendEntries;
      msg.from = id_;
      msg.to = peer_id;
      msg.term = term_;

      uint64_t prev_index = next_index - 1;
      msg.prev_log_index = prev_index;
      if (prev_index == 0) {
        msg.prev_log_term = 0;
      } else {
        msg.prev_log_term = log_entry(prev_index).term;
      }

      msg.entries.clear();
      for (uint64_t i = next_index; i <= last_log_index(); ++i) {
        msg.entries.push_back(log_entry(i));
      }

      msg.leader_commit = commit_index_;
      msgs_.push_back(msg);
    }
  }
}

proto::Message Raft::handle_append_entries(const proto::Message &msg) {
  proto::Message response;
  response.type = proto::MsgAppendEntriesResponse;
  response.from = id_;
  response.to = msg.from;
  response.success = false;

  // Update term if leader's term is higher
  if (msg.term > term_) {
    become_follower(msg.term, msg.from);
  }

  // Reject if term is lower
  if (msg.term < term_) {
    response.term = term_;
    return response;
  }

  reset_randomized_election_timeout();
  lead_ = msg.from;

  // Log consistency check
  bool log_ok = false;

  if (msg.prev_log_index == 0) {
    log_ok = true;
  } else if (last_log_index() >= msg.prev_log_index) {
    log_ok = log_entry(msg.prev_log_index).term == msg.prev_log_term;
  }

  response.term = term_;

  if (!log_ok) {
    response.match_index = 0;
    return response;
  }

  // Append entries to follower's log (starting from the match_index + 1)
  for (uint64_t i = 0; i < msg.entries.size(); ++i) {
    uint64_t index = msg.prev_log_index + i + 1;

    if (index <= last_log_index()) {
      if (log_entry(index).term != msg.entries[i].term) {
        // Conflict: truncate from this index onward, then append
        log_.erase(log_.begin() + (index - log_offset_ - 1), log_.end());
        log_.push_back(msg.entries[i]);

        // WAL: persist the new entry after conflict resolution
        if (wal_) {
          wal_->save_entry(msg.entries[i]);
        }
      }
    } else {
      log_.push_back(msg.entries[i]);

      // WAL: persist each new entry as it's appended
      if (wal_) {
        wal_->save_entry(msg.entries[i]);
      }
    }
  }

  // WAL: flush all entries in one sync (batched)
  if (wal_ && !msg.entries.empty()) {
    wal_->sync();
  }

  response.success = true;
  response.match_index = msg.prev_log_index + msg.entries.size();

  // Update commit index based on leader's commit
  {
    std::lock_guard<std::mutex> lock(apply_mutex_);
    if (msg.leader_commit > commit_index_) {
      uint64_t old_commit = commit_index_;
      commit_index_ =
          std::min(msg.leader_commit, last_log_index());

      // WAL: persist new commit index
      if (wal_) {
        wal_->save_hard_state({term_, voted_for_, commit_index_});
        wal_->sync();
      }

      // Push newly committed entries to lock-free queue
      push_entries_to_apply_queue(old_commit, commit_index_);
    }
  }

  return response;
}

void Raft::handle_append_entries_response(const proto::Message &msg) {
  if (state_ != State::Leader) {
    return;
  }

  if (term_ > msg.term) {
    return;
  }

  if (term_ < msg.term) {
    become_follower(msg.term, 0);
  }

  if (msg.success) {
    progress_[msg.from].match = msg.match_index;
    progress_[msg.from].next = progress_[msg.from].match + 1;

    // Track successful heartbeat response for ReadIndex
    if (read_index_pending_) {
      read_index_acks_[msg.from] = true;
    }

    // Try to advance commit index
    {
      std::lock_guard<std::mutex> lock(apply_mutex_);
      uint64_t old_commit = commit_index_;

      // Check each index from commit_index + 1 to last log index
      for (uint64_t i = commit_index_ + 1; i <= last_log_index(); ++i) {
        // Only commit entries from current term
        if (log_entry(i).term != term_) {
          continue;
        }

        // Count how many nodes have replicated this entry
        uint64_t replicas = 1; // Count self
        for (const auto &pair : progress_) {
          if (pair.second.match >= i) {
            replicas++;
          }
        }

        // If majority has replicated, commit it
        if (replicas > peers_.size() / 2) {
          commit_index_ = i;
        }
      }

      // Push newly committed entries to lock-free queue
      if (commit_index_ > old_commit) {
        // WAL: persist new commit index before applying
        if (wal_) {
          wal_->save_hard_state({term_, voted_for_, commit_index_});
          wal_->sync();
        }

        push_entries_to_apply_queue(old_commit, commit_index_);
      }
    }

  } else {
    // AppendEntries failed - follower doesn't have matching log entry
    // Decrement next and retry
    progress_[msg.from].next--;

    // Check if follower needs a snapshot (next index is already compacted)
    if (progress_[msg.from].next <= log_offset_) {
      // Follower needs entries we've already compacted - send snapshot
      proto::Message snap_msg;
      snap_msg.type = proto::MsgInstallSnapshot;
      snap_msg.from = id_;
      snap_msg.to = msg.from;
      snap_msg.term = term_;
      snap_msg.snapshot_index = log_offset_;
      snap_msg.snapshot_term = (log_offset_ > 0 && !log_.empty()) ? log_.front().term : 0;
      snap_msg.snapshot_data = last_snapshot_data_;  // Send cached snapshot

      msgs_.push_back(snap_msg);
    } else {
      // Follower just needs earlier entries - retry with AppendEntries
      broadcast_heartbeat();
    }
  }
}

void Raft::propose(const std::vector<uint8_t> &data) {
  if (state_ != State::Leader) {
    return;
  }

  proto::Entry entry;
  entry.type = proto::EntryNormal;
  entry.data = data;
  entry.index = last_log_index() + 1;
  entry.term = term_;

  log_.push_back(entry);

  // WAL: persist entry before broadcasting to followers
  if (wal_) {
    wal_->save_entry(entry);
    wal_->sync();
  }

  broadcast_heartbeat();
}

std::vector<proto::Entry> Raft::get_entries_to_apply() {
  std::lock_guard<std::mutex> lock(apply_mutex_);
  std::vector<proto::Entry> result;

  if (last_applied_ >= commit_index_) {
    return result;
  }

  for (uint64_t i = last_applied_ + 1; i <= commit_index_; ++i) {
    result.push_back(log_entry(i));
  }

  return result;
}

void Raft::advance(uint64_t index) {
  std::lock_guard<std::mutex> lock(apply_mutex_);
  if (index > commit_index_ || index <= last_applied_) {
    return;
  }
  last_applied_ = index;
}

void Raft::take_snapshot(const std::vector<uint8_t>& state_snapshot) {
  std::lock_guard<std::mutex> lock(apply_mutex_);

  // Can only snapshot up to last_applied_ (don't snapshot uncommitted state)
  if (last_applied_ == 0 || last_applied_ <= log_offset_) {
    return;  // Nothing to snapshot
  }

  // Get the term of the entry at last_applied_
  uint64_t snap_term = log_entry(last_applied_).term;

  // Create snapshot metadata
  wal::SnapshotMeta snap{last_applied_, snap_term, state_snapshot};

  // WAL-first: persist snapshot before truncating log
  if (wal_) {
    wal_->save_snapshot(snap);
    wal_->sync();
  }

  // Truncate log: keep only entries > last_applied_
  // Find array position of first entry to keep
  size_t keep_from = 0;
  for (size_t i = 0; i < log_.size(); ++i) {
    if (log_[i].index > last_applied_) {
      keep_from = i;
      break;
    }
  }

  // Erase everything before keep_from
  if (keep_from > 0) {
    log_.erase(log_.begin(), log_.begin() + keep_from);
  } else if (!log_.empty() && log_.back().index <= last_applied_) {
    // All entries are <= last_applied_, clear the entire log
    log_.clear();
  }

  // Update offset
  log_offset_ = last_applied_;
  last_snapshot_index_ = last_applied_;

  // Cache snapshot data for InstallSnapshot RPC
  last_snapshot_data_ = state_snapshot;
}

// Push newly committed entries to lock-free apply queue
// MUST be called with apply_mutex_ held
void Raft::push_entries_to_apply_queue(uint64_t old_commit, uint64_t new_commit) {
  // Push all entries in range (old_commit, new_commit]
  for (uint64_t i = old_commit + 1; i <= new_commit; ++i) {
    proto::Entry entry = log_entry(i); // Copy the entry

    // Try to enqueue (non-blocking)
    if (!apply_queue_->try_enqueue(std::move(entry))) {
      // Queue is full - this indicates backpressure
      // In production, we might want to:
      // 1. Log a warning
      // 2. Apply backpressure to clients (reject new writes)
      // 3. Increase queue size
      // For now, we'll just stop pushing and let apply thread catch up
      break;
    }
  }
}

// ReadIndex: Initiate a linearizable read
// Returns the commit index that should be applied before reading
uint64_t Raft::read_index() {
  // Only leader can serve ReadIndex
  if (state_ != State::Leader) {
    return 0; // Not leader, can't serve reads
  }

  // Mark ReadIndex as pending
  read_index_pending_ = true;
  pending_read_index_ = commit_index_;
  read_index_acks_.clear(); // Clear previous acks

  // Send heartbeat to confirm leadership
  broadcast_heartbeat();

  return pending_read_index_;
}

// Check if ReadIndex is confirmed (majority acked heartbeat)
bool Raft::read_index_ready(uint64_t read_index) {
  if (state_ != State::Leader) {
    return false;
  }

  // If this is not the pending read request, it's stale
  if (read_index != pending_read_index_) {
    return false;
  }

  // Need majority of peers to ack (including self)
  uint64_t quorum = peers_.size() / 2 + 1;

  // Count acks from unique peers (+ ourselves = 1)
  uint64_t ack_count = 1; // Count self
  for (const auto &ack : read_index_acks_) {
    if (ack.second) {
      ack_count++;
    }
  }

  return ack_count >= quorum;
}

// Start the async apply thread
void Raft::start_apply_thread(std::function<void(uint64_t, const std::vector<uint8_t>&)> state_machine) {
  // Check if already running
  if (apply_thread_running_.load()) {
    return;
  }

  state_machine_apply_ = state_machine;
  apply_thread_running_.store(true);

  // Launch the apply thread
  apply_thread_ = std::thread(&Raft::apply_thread_loop, this);
}

// Stop the async apply thread
void Raft::stop_apply_thread() {
  if (!apply_thread_running_.load()) {
    return;
  }

  // Signal thread to stop
  apply_thread_running_.store(false);

  // Wait for thread to exit
  if (apply_thread_.joinable()) {
    apply_thread_.join();
  }
}

// Apply thread main loop (consumer of lock-free queue)
void Raft::apply_thread_loop() {
  while (apply_thread_running_.load(std::memory_order_acquire)) {
    // Try to dequeue an entry (non-blocking)
    auto entry_opt = apply_queue_->try_dequeue();

    if (!entry_opt.has_value()) {
      // Queue is empty, sleep briefly to avoid busy-waiting
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      continue;
    }

    // We have an entry to apply
    proto::Entry entry = std::move(entry_opt.value());

    // Apply to state machine (user-provided callback)
    if (state_machine_apply_) {
      state_machine_apply_(entry.index, entry.data);
    }

    // Update last_applied index
    {
      std::lock_guard<std::mutex> lock(apply_mutex_);
      // Only update if this entry is the next one we expected
      // (entries must be applied in order!)
      if (entry.index == last_applied_ + 1) {
        last_applied_ = entry.index;
      }
    }
  }
}

// Handle InstallSnapshot RPC (follower receives snapshot from leader)
proto::Message Raft::handle_install_snapshot(const proto::Message &msg) {
  proto::Message response;
  response.type = proto::MsgInstallSnapshotResponse;
  response.from = id_;
  response.to = msg.from;
  response.success = false;

  // Update term if leader's term is higher
  if (msg.term > term_) {
    become_follower(msg.term, msg.from);
  }

  // Reject if term is lower
  if (msg.term < term_) {
    response.term = term_;
    return response;
  }

  reset_randomized_election_timeout();
  lead_ = msg.from;

  // Validate snapshot metadata
  if (msg.snapshot_index == 0 || msg.snapshot_data.empty()) {
    response.term = term_;
    return response; // Invalid snapshot
  }

  // If we already have entries beyond this snapshot, we don't need it
  // (This can happen if we received AppendEntries concurrently)
  if (last_applied_ >= msg.snapshot_index) {
    response.term = term_;
    response.success = true;
    response.match_index = last_applied_;
    return response;
  }

  {
    std::lock_guard<std::mutex> lock(apply_mutex_);

    // WAL: persist snapshot before modifying state
    if (wal_) {
      wal::SnapshotMeta snap{msg.snapshot_index, msg.snapshot_term, msg.snapshot_data};
      wal_->save_snapshot(snap);
      wal_->sync();
    }

    // Discard entire log and replace with snapshot
    log_.clear();
    log_offset_ = msg.snapshot_index;
    last_snapshot_index_ = msg.snapshot_index;

    // Update applied and commit indices
    last_applied_ = msg.snapshot_index;
    commit_index_ = msg.snapshot_index;

    // WAL: persist hard state after snapshot install
    if (wal_) {
      wal_->save_hard_state({term_, voted_for_, commit_index_});
      wal_->sync();
    }
  }

  response.term = term_;
  response.success = true;
  response.match_index = msg.snapshot_index;

  return response;
}

// Handle InstallSnapshot response (leader receives confirmation)
void Raft::handle_install_snapshot_response(const proto::Message &msg) {
  if (state_ != State::Leader) {
    return;
  }

  if (term_ > msg.term) {
    return;
  }

  if (term_ < msg.term) {
    become_follower(msg.term, 0);
    return;
  }

  if (msg.success) {
    // Update progress for this follower
    progress_[msg.from].match = msg.match_index;
    progress_[msg.from].next = progress_[msg.from].match + 1;

    // Try replicating remaining entries via AppendEntries
    broadcast_heartbeat();
  }
}

} // namespace kv