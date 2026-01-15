#include <common/bytebuffer.h>
#include <cstring>

namespace kv {

// OS page size is 4096 bytes
static const uint32_t MIN_BUFFERING = 4096;

ByteBuffer::ByteBuffer() : reader_(0), writer_(0), buff_(MIN_BUFFERING) {}

// Append data to buffer (grows if needed)
void ByteBuffer::put(const uint8_t *data, uint32_t len) {
  uint32_t available = buff_.size() - writer_;
  if (available < len) {
    buff_.resize(2 * buff_.size() + len);
  }
  memcpy(buff_.data() + writer_, data, len);
  writer_ += len;
}

// Mark data as consumed/read
void ByteBuffer::read_bytes(uint32_t len) {
  assert(readable_bytes() >= len);
  reader_ += len;
  may_shrink_to_fit();
}

// Gets pointer to start of unread data
const uint8_t *ByteBuffer::reader() const { return buff_.data() + reader_; }

// Check if there's data to read
bool ByteBuffer::readable() const { return writer_ > reader_; }

// How many bytes available to read
uint32_t ByteBuffer::readable_bytes() const {
  assert(writer_ >= reader_);
  return writer_ - reader_;
}

// Full reset to initial state
void ByteBuffer::reset() {
  buff_.resize(MIN_BUFFERING);
  writer_ = 0;
  reader_ = 0;
  buff_.shrink_to_fit();
}

void ByteBuffer::may_shrink_to_fit() {
  if (writer_ == reader_) {
    writer_ = 0;
    reader_ = 0;
  }
}

} // namespace kv
