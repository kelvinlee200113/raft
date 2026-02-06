#pragma once
#include <msgpack.hpp>
#include <raft/proto.h>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <vector>

namespace kv {

class KVStore {
public:
  KVStore() = default;

  bool get(const std::string &key, std::string &value) const;
  void set(const std::string &key, const std::string &value);
  void del(const std::string &key);
  void apply(const proto::Entry &entry);

  // Snapshot support: serialize entire store to bytes / replace store from bytes
  std::vector<uint8_t> serialize() const;
  void deserialize(const std::vector<uint8_t>& data);

private:
  std::unordered_map<std::string, std::string> store_;
};

// Command types for KV operations
enum class CommandType : uint8_t { Set = 0, Del = 1 };

// Command structure for KV operations
struct Command {
  CommandType type;
  std::vector<std::string> strs;
  MSGPACK_DEFINE(type, strs);
};

} // namespace kv

namespace msgpack {

MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
  namespace adaptor {

  template <> struct pack<kv::CommandType> {
    template <typename Stream>
    msgpack::packer<Stream> &operator()(msgpack::packer<Stream> &o,
                                        kv::CommandType const &v) const {
      return o.pack(static_cast<uint8_t>(v));
    }
  };

  template <> struct convert<kv::CommandType> {
    msgpack::object const &operator()(msgpack::object const &o,
                                      kv::CommandType &v) const {
      if (o.type != msgpack::type::POSITIVE_INTEGER) {
        throw msgpack::type_error();
      }
      v = static_cast<kv::CommandType>(o.via.u64);
      return o;
    }
  };

  } // namespace adaptor
}
} // namespace msgpack