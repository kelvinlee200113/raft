#include <server/kv_store.h>

namespace kv {

bool KVStore::get(const std::string &key, std::string &value) const {
  auto it = store_.find(key);
  if (it != store_.end()) {
    value = it->second;
    return true;
  }
  return false;
}

void KVStore::set(const std::string &key, const std::string &value) {
  store_[key] = value;
}

void KVStore::del(const std::string &key) { store_.erase(key); }

void KVStore::apply(const proto::Entry &entry) {
  // TODO: Implement deserialization

  // Get the command

  // 1. Entry.data is binary, need to unpack to msgpack::object via unpack()
  msgpack::object_handle oh = msgpack::unpack(
      reinterpret_cast<const char *>(entry.data.data()), entry.data.size());
  msgpack::object obj = oh.get();

  // 2. Then call msgpack::as to call the implicit convert() to convert to
  // desired command struct

  Command cmd = obj.as<Command>();

  // 3. Apply command to the store_ by calling one of get()/set()/del()
  if (cmd.type == CommandType::Set) {
    // key = cmd.strs[0], value = cmd.strs[1]
    set(cmd.strs[0], cmd.strs[1]);
  } else if (cmd.type == CommandType::Del) {
    for (const auto &key : cmd.strs) {
      del(key);
    }
  }
}

} // namespace kv
