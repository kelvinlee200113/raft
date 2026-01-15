#pragma once
#include <cstdint>

namespace kv {

// Transport type constant
const uint8_t TransportTypeStream = 1;

// Message framing header
// Wire format: [1 byte type][4 bytes length][data...]
// Removes padding to resolve ambiguity when reading network bytestream
#pragma pack(1)
struct TransportMeta {
  uint8_t type; // Message type (always TransportTypeStream for now)
  uint32_t len; // Length of following data (in network byte order)
};
#pragma pack()

} // namespace kv
