#include <gtest/gtest.h>
#include <common/lock_free_queue.h>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>

using namespace kv;

// Test basic enqueue/dequeue operations
TEST(LockFreeQueueTest, BasicEnqueueDequeue) {
  LockFreeQueue<int> queue(10);

  // Enqueue some items
  EXPECT_TRUE(queue.try_enqueue(1));
  EXPECT_TRUE(queue.try_enqueue(2));
  EXPECT_TRUE(queue.try_enqueue(3));

  EXPECT_EQ(queue.size(), 3);
  EXPECT_FALSE(queue.empty());

  // Dequeue items
  auto val1 = queue.try_dequeue();
  ASSERT_TRUE(val1.has_value());
  EXPECT_EQ(val1.value(), 1);

  auto val2 = queue.try_dequeue();
  ASSERT_TRUE(val2.has_value());
  EXPECT_EQ(val2.value(), 2);

  auto val3 = queue.try_dequeue();
  ASSERT_TRUE(val3.has_value());
  EXPECT_EQ(val3.value(), 3);

  // Queue should be empty
  EXPECT_TRUE(queue.empty());
  auto empty = queue.try_dequeue();
  EXPECT_FALSE(empty.has_value());
}

// Test that queue respects capacity
TEST(LockFreeQueueTest, RespectCapacity) {
  LockFreeQueue<int> queue(3);

  // Fill the queue
  EXPECT_TRUE(queue.try_enqueue(1));
  EXPECT_TRUE(queue.try_enqueue(2));
  EXPECT_TRUE(queue.try_enqueue(3));

  // Next enqueue should fail (queue full)
  EXPECT_FALSE(queue.try_enqueue(4));

  // Dequeue one item
  auto val = queue.try_dequeue();
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(val.value(), 1);

  // Now we should be able to enqueue again
  EXPECT_TRUE(queue.try_enqueue(4));

  // But not twice
  EXPECT_FALSE(queue.try_enqueue(5));
}

// Test FIFO ordering
TEST(LockFreeQueueTest, FIFOOrdering) {
  LockFreeQueue<int> queue(100);

  // Enqueue 100 items
  for (int i = 0; i < 100; i++) {
    EXPECT_TRUE(queue.try_enqueue(i));
  }

  // Dequeue and verify order
  for (int i = 0; i < 100; i++) {
    auto val = queue.try_dequeue();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), i);
  }

  EXPECT_TRUE(queue.empty());
}

// Test concurrent producer-consumer (SPSC)
TEST(LockFreeQueueTest, ConcurrentProducerConsumer) {
  const int NUM_ITEMS = 100000;
  LockFreeQueue<int> queue(10000);

  std::atomic<bool> producer_done{false};
  std::atomic<int> items_consumed{0};

  // Producer thread
  std::thread producer([&]() {
    for (int i = 0; i < NUM_ITEMS; i++) {
      while (!queue.try_enqueue(i)) {
        // Queue full, spin until space available
        std::this_thread::yield();
      }
    }
    producer_done.store(true);
  });

  // Consumer thread
  std::thread consumer([&]() {
    int expected = 0;
    while (expected < NUM_ITEMS) {
      auto val = queue.try_dequeue();
      if (val.has_value()) {
        EXPECT_EQ(val.value(), expected);
        expected++;
        items_consumed.fetch_add(1);
      } else {
        // Queue empty, yield CPU
        std::this_thread::yield();
      }
    }
  });

  producer.join();
  consumer.join();

  EXPECT_EQ(items_consumed.load(), NUM_ITEMS);
  EXPECT_TRUE(queue.empty());
}

// Benchmark: Measure throughput
TEST(LockFreeQueueTest, ThroughputBenchmark) {
  const int NUM_ITEMS = 1000000;
  LockFreeQueue<int> queue(10000);

  std::atomic<bool> producer_done{false};

  auto start = std::chrono::high_resolution_clock::now();

  // Producer thread
  std::thread producer([&]() {
    for (int i = 0; i < NUM_ITEMS; i++) {
      while (!queue.try_enqueue(i)) {
        std::this_thread::yield();
      }
    }
    producer_done.store(true);
  });

  // Consumer thread
  std::thread consumer([&]() {
    int consumed = 0;
    while (consumed < NUM_ITEMS) {
      auto val = queue.try_dequeue();
      if (val.has_value()) {
        consumed++;
      } else {
        std::this_thread::yield();
      }
    }
  });

  producer.join();
  consumer.join();

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  double ops_per_sec = (NUM_ITEMS * 2.0) / (duration.count() / 1000.0);

  std::cout << "\n=== Lock-Free Queue Benchmark ===" << std::endl;
  std::cout << "Items: " << NUM_ITEMS << std::endl;
  std::cout << "Time: " << duration.count() << " ms" << std::endl;
  std::cout << "Throughput: " << (ops_per_sec / 1000000.0) << " M ops/sec" << std::endl;
  std::cout << "Latency: " << (duration.count() * 1000000.0 / NUM_ITEMS) << " ns/op" << std::endl;

  // On modern hardware, expect > 10M ops/sec
  EXPECT_GT(ops_per_sec, 1000000.0); // At least 1M ops/sec
}

// Test with complex types (move semantics)
TEST(LockFreeQueueTest, MoveSemantics) {
  LockFreeQueue<std::vector<int>> queue(10);

  // Create a large vector
  std::vector<int> vec;
  for (int i = 0; i < 1000; i++) {
    vec.push_back(i);
  }

  // Move it into the queue
  EXPECT_TRUE(queue.try_enqueue(std::move(vec)));

  // Original vector should be empty (moved from)
  EXPECT_TRUE(vec.empty());

  // Dequeue and verify
  auto result = queue.try_dequeue();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().size(), 1000);
  EXPECT_EQ(result.value()[0], 0);
  EXPECT_EQ(result.value()[999], 999);
}

// Test wrap-around behavior (ring buffer)
TEST(LockFreeQueueTest, WrapAround) {
  LockFreeQueue<int> queue(5);

  // Fill and drain multiple times to test wrap-around
  for (int round = 0; round < 10; round++) {
    // Fill queue
    for (int i = 0; i < 5; i++) {
      EXPECT_TRUE(queue.try_enqueue(round * 10 + i));
    }

    // Drain queue
    for (int i = 0; i < 5; i++) {
      auto val = queue.try_dequeue();
      ASSERT_TRUE(val.has_value());
      EXPECT_EQ(val.value(), round * 10 + i);
    }

    EXPECT_TRUE(queue.empty());
  }
}

// Stress test: Rapid enqueue/dequeue cycles
TEST(LockFreeQueueTest, StressTest) {
  const int ROUNDS = 10000;
  LockFreeQueue<int> queue(100);

  std::atomic<bool> running{true};
  std::atomic<uint64_t> enqueued{0};
  std::atomic<uint64_t> dequeued{0};

  // Producer
  std::thread producer([&]() {
    for (int i = 0; i < ROUNDS; i++) {
      while (!queue.try_enqueue(i)) {
        std::this_thread::yield();
      }
      enqueued.fetch_add(1);
    }
  });

  // Consumer
  std::thread consumer([&]() {
    int expected = 0;
    while (expected < ROUNDS) {
      auto val = queue.try_dequeue();
      if (val.has_value()) {
        EXPECT_EQ(val.value(), expected);
        expected++;
        dequeued.fetch_add(1);
      } else {
        std::this_thread::yield();
      }
    }
  });

  producer.join();
  consumer.join();

  EXPECT_EQ(enqueued.load(), ROUNDS);
  EXPECT_EQ(dequeued.load(), ROUNDS);
  EXPECT_TRUE(queue.empty());
}

// Test that lock-free queue works with Raft Entry
#include <raft/proto.h>

TEST(LockFreeQueueTest, RaftEntries) {
  LockFreeQueue<proto::Entry> queue(100);

  // Create entries
  for (int i = 0; i < 10; i++) {
    proto::Entry entry;
    entry.type = proto::EntryNormal;
    entry.term = 1;
    entry.index = i + 1;
    entry.data = {static_cast<uint8_t>(i), static_cast<uint8_t>(i+1)};

    EXPECT_TRUE(queue.try_enqueue(std::move(entry)));
  }

  // Verify entries
  for (int i = 0; i < 10; i++) {
    auto entry = queue.try_dequeue();
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry.value().index, i + 1);
    EXPECT_EQ(entry.value().term, 1);
    EXPECT_EQ(entry.value().data.size(), 2);
  }

  EXPECT_TRUE(queue.empty());
}
