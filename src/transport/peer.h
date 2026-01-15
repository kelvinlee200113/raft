#pragma once
#include <memory>
#include <raft/proto.h>

namespace kv {

class Peer {
public:
  virtual ~Peer() = default;

  virtual void start() = 0;
  virtual void send(proto::MessagePtr msg) = 0;
  virtual void stop() = 0;

  static std::shared_ptr<Peer>
  create(uint64_t peer_id, const std::string &peer_addr, void *io_service);
};

typedef std::shared_ptr<Peer> PeerPtr;

} // namespace kv