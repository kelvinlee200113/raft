#pragma once
#include <cassert>
#include <cstdint>
#include <vector>

namespace kv {

// ByteBuffer: Growing buffer with read/write pointers
// Avoids O(N) erase by tracking reader position
class ByteBuffer {
public:
  ByteBuffer();

  // Append data to buffer (grows if needed)
  void put(const uint8_t *data, uint32_t len);

  // Mark data as consumed/read
  void read_bytes(uint32_t len);

  // Gets pointer to start of unread data
  const uint8_t *reader() const;

  // Check if there's data to read
  bool readable() const;

  // How many bytes available to read
  uint32_t readable_bytes() const;

  // Full reset to initial state
  void reset();

private:
  // Reset pointers to 0 when buffer fully consumed
  void may_shrink_to_fit();

  uint32_t reader_;
  uint32_t writer_;
  std::vector<uint8_t> buff_;
};

} // namespace kv