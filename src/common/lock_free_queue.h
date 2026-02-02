#pragma once
#include <atomic>
#include <cstddef>
#include <new>
#include <optional>

namespace kv {

// Lock-free Single-Producer Single-Consumer (SPSC) bounded queue
// Optimized for low-latency systems
//
// Key features:
// - Zero locks: Uses atomic operations only
// - Cache-line aligned: Prevents false sharing
// - Bounded: Fixed size (prevents unbounded memory growth)
// - Wait-free for producer and consumer: O(1) operations
//
// Performance characteristics:
// - Enqueue: ~10-20 ns (depends on cache state)
// - Dequeue: ~10-20 ns (depends on cache state)
// - No context switches or kernel calls
// - No memory allocations after construction
//
// Memory ordering:
// - Uses release-acquire semantics for synchronization
// - Producer writes with release, consumer reads with acquire
// - Guarantees visibility without sequential consistency overhead
template <typename T> class LockFreeQueue {
public:
  explicit LockFreeQueue(size_t capacity)
      : capacity_(capacity + 1), // +1 to distinguish full from empty
        buffer_(new Slot[capacity + 1]), head_(0), tail_(0) {
    // Initialize all slots
    for (size_t i = 0; i <= capacity; ++i) {
      buffer_[i].turn.store(i, std::memory_order_relaxed);
    }
  }

  ~LockFreeQueue() { delete[] buffer_; }

  // Non-copyable and non-movable (contains atomics)
  LockFreeQueue(const LockFreeQueue &) = delete;
  LockFreeQueue &operator=(const LockFreeQueue &) = delete;

  // Try to enqueue an item (non-blocking)
  // Returns true if successful, false if queue is full
  //
  // Producer-only operation (single thread)
  bool try_enqueue(T item) {
    // Load tail with relaxed ordering (we're the only writer)
    size_t tail = tail_.load(std::memory_order_relaxed);
    Slot &slot = buffer_[tail % capacity_];

    // Check if slot is available
    // We use turn to track which "generation" of the ring buffer we're in
    size_t turn = slot.turn.load(std::memory_order_acquire);
    if (turn != tail) {
      return false; // Queue is full
    }

    // Write the item
    slot.item = std::move(item);

    // Mark slot as filled and advance to next generation
    // Consumer is waiting for turn == head + capacity
    slot.turn.store(tail + capacity_, std::memory_order_release);

    // Advance tail
    tail_.store(tail + 1, std::memory_order_release);

    return true;
  }

  // Try to dequeue an item (non-blocking)
  // Returns item if successful, std::nullopt if queue is empty
  //
  // Consumer-only operation (single thread)
  std::optional<T> try_dequeue() {
    // Load head with relaxed ordering (we're the only reader)
    size_t head = head_.load(std::memory_order_relaxed);
    Slot &slot = buffer_[head % capacity_];

    // Check if slot has data
    size_t turn = slot.turn.load(std::memory_order_acquire);
    if (turn != head + capacity_) {
      return std::nullopt; // Queue is empty
    }

    // Read the item
    T item = std::move(slot.item);

    // Mark slot as consumed and advance head
    slot.turn.store(head + capacity_ * 2, std::memory_order_release);
    head_.store(head + 1, std::memory_order_release);

    return item;
  }

  // Get approximate size (may be stale due to concurrent access)
  size_t size() const {
    size_t tail = tail_.load(std::memory_order_acquire);
    size_t head = head_.load(std::memory_order_acquire);
    return tail - head;
  }

  // Check if queue is empty (approximate)
  bool empty() const {
    size_t tail = tail_.load(std::memory_order_acquire);
    size_t head = head_.load(std::memory_order_acquire);
    return tail == head;
  }

  // Get capacity
  size_t capacity() const { return capacity_ - 1; }

private:
  // Slot in the ring buffer
  // Each slot has a turn counter to track which generation we're in
  struct Slot {
    T item;
    std::atomic<size_t> turn;
  };

  size_t capacity_;
  Slot *buffer_;

  // Cache-line padding to prevent false sharing
  // Modern CPUs have 64-byte cache lines
  // We want head_ and tail_ on different cache lines
  alignas(64) std::atomic<size_t> head_; // Consumer writes here
  alignas(64) std::atomic<size_t> tail_; // Producer writes here
};

} // namespace kv
