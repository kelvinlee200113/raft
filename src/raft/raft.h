#pragma once
#include <raft/config.h>
#include <raft/proto.h>
#include <wal/wal.h>
#include <common/lock_free_queue.h>
#include <stdint.h>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <functional>

namespace kv {

// Raft node states
enum class State { Follower, PreCandidate, Candidate, Leader };

// Progress tracks replication progress for each follower
struct Progress {
  uint64_t match; // Highest log index known to be replicated on this follower
  uint64_t next;  // Next log index to send to this follower

  // Logs are 1-indexed
  Progress() : match(0), next(1) {}
};

class Raft {
public:
  explicit Raft(const Config &config);

  void become_follower(uint64_t term, uint64_t leader);

  void become_pre_candidate();

  void become_candidate();

  void become_leader();

  void tick();

  void reset_randomized_election_timeout();

  proto::Message handle_request_vote(const proto::Message &msg);

  void handle_request_vote_response(const proto::Message &msg);

  void campaign();

  void pre_campaign();

  proto::Message handle_pre_vote(const proto::Message &msg);

  void handle_pre_vote_response(const proto::Message &msg);

  void send(proto::Message msg);

  std::vector<proto::Message> read_messages();

  void broadcast_heartbeat();

  proto::Message handle_append_entries(const proto::Message &msg);

  void handle_append_entries_response(const proto::Message &msg);

  proto::Message handle_install_snapshot(const proto::Message &msg);

  void handle_install_snapshot_response(const proto::Message &msg);

  void propose(const std::vector<uint8_t> &data);

  // Legacy mutex-based API (for backward compatibility)
  std::vector<proto::Entry> get_entries_to_apply();
  void advance(uint64_t index);

  // Lock-free API for high-performance async apply
  // Returns pointer to apply queue (producer pushes, consumer pops)
  LockFreeQueue<proto::Entry>* get_apply_queue() { return apply_queue_.get(); }

  // Start the async apply thread
  // state_machine: Callback function to apply an entry
  // The callback receives (index, data) and should apply the entry to the state machine
  void start_apply_thread(std::function<void(uint64_t, const std::vector<uint8_t>&)> state_machine);

  // Stop the async apply thread (blocks until thread exits)
  void stop_apply_thread();

  // Snapshot: Compact log by taking a snapshot of the state machine
  // state_snapshot: serialized state machine bytes (from KVStore::serialize())
  void take_snapshot(const std::vector<uint8_t>& state_snapshot);

  // Check if a snapshot should be taken (threshold crossed)
  // Returns true if (last_applied - last_snapshot_index) >= threshold
  bool should_snapshot() const {
    return snapshot_threshold_ > 0 &&
           (last_applied_ - last_snapshot_index_) >= snapshot_threshold_;
  }

  // ReadIndex: Linearizable reads without going through the log
  // Returns the commit index that can be safely read once confirmed
  uint64_t read_index();

  // Check if ReadIndex confirmation is ready (majority responded to heartbeat)
  bool read_index_ready(uint64_t read_index);

  // Attach a WAL for crash recovery (optional — tests may omit this)
  void set_wal(std::unique_ptr<wal::WAL> w) { wal_ = std::move(w); }

  // Restore Raft state from WAL recovery (call once at startup, before event loop)
  // Loads log entries and HardState atomically. last_applied stays at 0 —
  // caller must replay entries [1..commit_index] into the state machine.
  void restore(const wal::HardStateProto& hard_state, const std::vector<proto::Entry>& entries);

  // Test helpers: For testing only
  void test_set_commit_index(uint64_t index) {
    std::lock_guard<std::mutex> lock(apply_mutex_);
    uint64_t old_commit = commit_index_;
    commit_index_ = index;
    // Push newly committed entries to queue (for async apply)
    if (index > old_commit) {
      push_entries_to_apply_queue(old_commit, index);
    }
  }
  void test_append_log_entry(const proto::Entry& entry) { log_.push_back(entry); }
  size_t test_get_log_size() const { return log_.size(); }

  uint64_t get_term() const { return term_; }
  uint64_t get_id() const { return id_; }
  uint64_t get_leader() const { return lead_; }
  uint64_t get_voted_for() const { return voted_for_; }
  uint64_t get_commit_index() const { return commit_index_; }
  uint64_t get_last_applied() const { return last_applied_; }
  State get_state() const { return state_; }
  const std::vector<proto::Entry> &get_log() const { return log_; }
  uint64_t get_log_offset() const { return log_offset_; }
  const std::unordered_map<uint64_t, Progress> &get_progress() const {
    return progress_;
  }
  const std::vector<uint64_t> &get_peers() const { return peers_; }
  uint32_t get_election_timeout() const { return election_timeout_; }
  uint32_t get_heartbeat_timeout() const { return heartbeat_timeout_; }
  uint32_t get_election_elapsed() const { return election_elapsed_; }
  uint32_t get_randomized_election_timeout() const {
    return randomized_election_timeout_;
  }
  const std::unordered_map<uint64_t, bool> &get_votes() const { return votes_; }

private:
  // Helper: Push newly committed entries to apply queue
  // Must be called with apply_mutex_ held
  void push_entries_to_apply_queue(uint64_t old_commit, uint64_t new_commit);

  // Log index helpers (account for log_offset_ after compaction)
  uint64_t last_log_index() const { return log_offset_ + log_.size(); }
  const proto::Entry& log_entry(uint64_t index) const { return log_[index - log_offset_ - 1]; }

  uint64_t id_;
  uint64_t term_;
  uint64_t lead_;
  uint64_t voted_for_;
  uint64_t commit_index_;
  uint64_t last_applied_;
  State state_;
  std::vector<proto::Entry> log_;
  uint64_t log_offset_;  // Index of last compacted entry (0 = nothing compacted)
  std::unordered_map<uint64_t, Progress> progress_;
  std::vector<uint64_t> peers_;

  // Timeout configuration
  uint32_t election_timeout_;  // Base election timeout in ticks
  uint32_t heartbeat_timeout_; // Heartbeat interval in ticks

  // Snapshot configuration
  uint64_t snapshot_threshold_;   // Take snapshot every N applied entries (0 = disabled)
  uint64_t last_snapshot_index_;  // Index of last snapshot taken

  // Election timing
  uint32_t election_elapsed_;            // Ticks since last reset
  uint32_t randomized_election_timeout_; // Random timeout for this election
  uint32_t heartbeat_elapsed_;           // Ticks since last heartbeat

  // Voting
  std::unordered_map<uint64_t, bool>
      votes_; // Track votes received (node_id -> granted)
  std::unordered_map<uint64_t, bool>
      pre_votes_; // Track pre-votes received (node_id -> granted)

  // ReadIndex state
  bool read_index_pending_;               // Is there a pending ReadIndex request?
  uint64_t pending_read_index_;           // Commit index when read was requested
  std::unordered_map<uint64_t, bool> read_index_acks_;  // Track which peers acked

  // Snapshot cache (for InstallSnapshot RPC)
  std::vector<uint8_t> last_snapshot_data_;  // Cached snapshot data to send to followers

  // WAL for crash recovery (nullptr if not attached)
  std::unique_ptr<wal::WAL> wal_;

  // Outgoing messages queue
  std::vector<proto::Message> msgs_;

  // Thread safety for async apply
  mutable std::mutex apply_mutex_;  // Protects last_applied_ and commit_index_

  // Lock-free queue for async apply (SPSC: Raft thread -> Apply thread)
  // Capacity of 10000 entries (~10MB for typical entries)
  std::unique_ptr<LockFreeQueue<proto::Entry>> apply_queue_;

  // Async apply thread
  std::thread apply_thread_;
  std::atomic<bool> apply_thread_running_;
  std::function<void(uint64_t, const std::vector<uint8_t>&)> state_machine_apply_;

  // Apply thread main loop
  void apply_thread_loop();
};

} // namespace kv
