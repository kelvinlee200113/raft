#include <boost/asio.hpp>
#include <chrono>
#include <iostream>
#include <map>
#include <raft/config.h>
#include <raft/raft.h>
#include <server/kv_store.h>
#include <sstream>
#include <string>
#include <transport/peer.h>
#include <transport/server.h>
#include <vector>
#include <thread>
#include <atomic>
#include <msgpack.hpp>

// Parse command-line arguments
struct NodeConfig {
  uint64_t id;
  std::string listen_addr;
  std::vector<std::string> peer_addrs;
};

void print_usage(const char *program) {
  std::cerr << "Usage: " << program
            << " --id=<node_id> --listen=<ip:port> "
               "--peers=<ip:port>,<ip:port>,..."
            << std::endl;
  std::cerr << "Example: " << program
            << " --id=1 --listen=127.0.0.1:9001 "
               "--peers=127.0.0.1:9002,127.0.0.1:9003"
            << std::endl;
}

NodeConfig parse_args(int argc, char **argv) {
  NodeConfig config;
  config.id = 0;

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];

    if (arg.find("--id=") == 0) {
      config.id = std::stoull(arg.substr(5));
    } else if (arg.find("--listen=") == 0) {
      config.listen_addr = arg.substr(9);
    } else if (arg.find("--peers=") == 0) {
      std::string peers_str = arg.substr(8);
      std::stringstream ss(peers_str);
      std::string peer;
      while (std::getline(ss, peer, ',')) {
        config.peer_addrs.push_back(peer);
      }
    }
  }

  if (config.id == 0 || config.listen_addr.empty() ||
      config.peer_addrs.empty()) {
    print_usage(argv[0]);
    exit(1);
  }

  return config;
}

// Legacy mutex-based apply loop (for testing/comparison)
void apply_loop_mutex(kv::Raft* raft, kv::KVStore* kv_store,
                      std::atomic<bool>* running, uint64_t node_id) {
  while (running->load()) {
    auto entries = raft->get_entries_to_apply();

    if (entries.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    for (const auto& entry : entries) {
      if (entry.type == kv::proto::EntryNormal) {
        kv_store->apply(entry);
        std::cout << "[Node " << node_id << "] Applied entry "
                  << entry.index << " (mutex)" << std::endl;
      }
      raft->advance(entry.index);
    }
  }
}

// Lock-free apply loop - HIGH PERFORMANCE
// Uses lock-free SPSC queue for zero-lock async apply
// Perfect for high-frequency trading and low-latency systems
void apply_loop(kv::Raft* raft, kv::KVStore* kv_store,
                std::atomic<bool>* running, uint64_t node_id) {
  auto* queue = raft->get_apply_queue();

  while (running->load()) {
    // Try to dequeue an entry (non-blocking, ~10-20ns)
    auto maybe_entry = queue->try_dequeue();

    if (!maybe_entry.has_value()) {
      // Queue is empty, briefly yield CPU to avoid spinning
      // In HFT systems, you might use _mm_pause() or busy-wait instead
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      continue;
    }

    // Apply entry to state machine
    const auto& entry = maybe_entry.value();
    if (entry.type == kv::proto::EntryNormal) {
      kv_store->apply(entry);
      std::cout << "[Node " << node_id << "] Applied entry "
                << entry.index << " (lock-free)" << std::endl;
    }

    // Mark as applied
    raft->advance(entry.index);
  }

  std::cout << "[Node " << node_id << "] Lock-free apply thread stopped" << std::endl;
}

int main(int argc, char **argv) {
  // Parse command-line arguments
  NodeConfig node_config = parse_args(argc, argv);

  std::cout << "=== Starting Raft Node ===" << std::endl;
  std::cout << "Node ID: " << node_config.id << std::endl;
  std::cout << "Listen address: " << node_config.listen_addr << std::endl;
  std::cout << "Peer addresses: ";
  for (const auto &addr : node_config.peer_addrs) {
    std::cout << addr << " ";
  }
  std::cout << std::endl;

  // Create io_context for async I/O
  boost::asio::io_context io_ctx;

  // Create Raft configuration
  kv::Config raft_config;
  raft_config.id = node_config.id;
  // All node IDs (1, 2, 3, ...)
  for (size_t i = 0; i < node_config.peer_addrs.size() + 1; i++) {
    raft_config.peers.push_back(i + 1);
  }
  raft_config.election_tick = 10;   // 10 ticks * 100ms = 1 second base timeout
  raft_config.heartbeat_tick = 1;   // 1 tick * 100ms = 100ms heartbeat

  // Create Raft instance
  kv::Raft raft(raft_config);

  // Create/open WAL for crash recovery
  std::string wal_dir = "./wal_data/" + std::to_string(node_config.id);
  auto wal_ptr = kv::wal::WAL::open(wal_dir);
  if (!wal_ptr) {
    // First run — no existing WAL, create fresh
    wal_ptr = kv::wal::WAL::create(wal_dir);
  } else {
    // Recovering — replay WAL into Raft state
    std::vector<kv::proto::Entry> entries;
    auto hard_state = wal_ptr->recover(entries);
    if (!hard_state.is_empty()) {
      std::cout << "WAL recovered: term=" << hard_state.term
                << " vote=" << hard_state.vote
                << " commit=" << hard_state.commit
                << " entries=" << entries.size() << std::endl;
    }
  }
  raft.set_wal(std::move(wal_ptr));
  std::cout << "WAL ready at " << wal_dir << std::endl;

  // Create KV store
  kv::KVStore kv_store;

  std::cout << "Raft initialized: term=" << raft.get_term()
            << " state=Follower" << std::endl;

  // Start built-in async apply thread with KVStore callback
  raft.start_apply_thread([&kv_store, node_id = node_config.id](uint64_t index, const std::vector<uint8_t>& data) {
    // Create entry for apply (KVStore expects proto::Entry)
    kv::proto::Entry entry;
    entry.index = index;
    entry.data = data;
    entry.type = kv::proto::EntryNormal;

    // Apply to state machine
    kv_store.apply(entry);

    // Log the apply operation
    std::cout << "[Node " << node_id << "] Applied entry " << index << std::endl;
  });
  std::cout << "Started async apply thread (lock-free)" << std::endl;

  // Create Server for incoming connections
  auto server = kv::Server::create(io_ctx, node_config.listen_addr, &raft);
  server->start();
  std::cout << "Server listening on " << node_config.listen_addr << std::endl;

  // Create Peers for outgoing connections
  std::map<uint64_t, std::shared_ptr<kv::Peer>> peers;
  for (size_t i = 0; i < node_config.peer_addrs.size(); i++) {
    uint64_t peer_id = i + 1;
    if (peer_id >= node_config.id) {
      peer_id++; // Skip our own ID
    }
    peers[peer_id] =
        kv::Peer::create(peer_id, node_config.peer_addrs[i], &io_ctx);
    std::cout << "Created peer " << peer_id << " -> "
              << node_config.peer_addrs[i] << std::endl;
  }

  // Periodic tick timer (100ms)
  boost::asio::steady_timer timer(io_ctx);
  std::function<void()> tick_handler;

  tick_handler = [&]() {
    // 1. Tick Raft state machine
    raft.tick();

    // 2. Send outgoing messages from Raft
    auto messages = raft.read_messages();
    for (auto &msg : messages) {
      if (peers.count(msg.to)) {
        peers[msg.to]->send(std::make_shared<kv::proto::Message>(msg));
      }
    }

    // 3. Print status every 10 ticks (1 second)
    static int tick_count = 0;
    tick_count++;
    if (tick_count % 10 == 0) {
      std::string state_str =
          raft.get_state() == kv::State::Follower
              ? "Follower"
              : (raft.get_state() == kv::State::Candidate ? "Candidate"
                                                           : "Leader");
      std::cout << "[Node " << node_config.id << "] "
                << "Term=" << raft.get_term() << " State=" << state_str
                << " Leader=" << raft.get_leader()
                << " Commit=" << raft.get_commit_index()
                << " Applied=" << raft.get_last_applied() << std::endl;
    }

    // 4. Schedule next tick (100ms)
    timer.expires_after(std::chrono::milliseconds(100));
    timer.async_wait([&](const boost::system::error_code &ec) {
      if (!ec) {
        tick_handler();
      }
    });
  };

  // Start the tick timer
  tick_handler();

  std::cout << "\n=== Node " << node_config.id << " Running ===" << std::endl;
  std::cout << "Press Ctrl+C to stop" << std::endl;

  // Run io_context event loop (blocks until stopped)
  io_ctx.run();

  // Cleanup: Stop apply thread
  std::cout << "\nShutting down..." << std::endl;
  raft.stop_apply_thread();
  std::cout << "Apply thread stopped" << std::endl;

  return 0;
}
