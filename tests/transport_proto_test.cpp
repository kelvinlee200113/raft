#include <gtest/gtest.h>
#include <transport/proto.h>
#include <cstring>

using namespace kv;

// Test that TransportMeta has correct size (no padding)
TEST(TransportProtoTest, MetaSize) {
  EXPECT_EQ(sizeof(TransportMeta), 5);  // 1 byte type + 4 bytes len
}

// Test that we can create and initialize TransportMeta
TEST(TransportProtoTest, MetaInitialization) {
  TransportMeta meta;
  meta.type = TransportTypeStream;
  meta.len = 100;

  EXPECT_EQ(meta.type, 1);
  EXPECT_EQ(meta.len, 100);
}

// Test network byte order conversion (htonl/ntohl)
TEST(TransportProtoTest, NetworkByteOrder) {
  TransportMeta meta;
  meta.type = TransportTypeStream;

  uint32_t host_len = 1000;
  meta.len = htonl(host_len);  // Convert to network byte order

  // On little-endian machine, bytes should be reversed
  uint32_t network_len = meta.len;
  uint32_t back_to_host = ntohl(network_len);

  EXPECT_EQ(back_to_host, host_len);
}

// Test that struct packing works (no padding between fields)
TEST(TransportProtoTest, StructPacking) {
  TransportMeta meta;

  // Set values
  meta.type = 0xAB;
  meta.len = 0x12345678;

  // Cast to bytes to verify memory layout
  uint8_t* bytes = reinterpret_cast<uint8_t*>(&meta);

  // First byte should be type
  EXPECT_EQ(bytes[0], 0xAB);

  // Next 4 bytes should be len (in whatever byte order we set)
  // Just verify we can read them back
  TransportMeta* meta_ptr = reinterpret_cast<TransportMeta*>(bytes);
  EXPECT_EQ(meta_ptr->type, 0xAB);
  EXPECT_EQ(meta_ptr->len, 0x12345678);
}
