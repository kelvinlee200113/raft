#include <boost/asio.hpp>
#include <iostream>
#include <msgpack.hpp>
#include <string>
#include <transport/peer.h>
#include <transport/proto.h>
#include <vector>

namespace kv {

class PeerImpl;

// ClientSession manages ONE TCP connection to a remote peer
class ClientSession {
public:
  ClientSession(boost::asio::io_context &io_ctx, PeerImpl *peer,
                uint64_t peer_id, boost::asio::ip::tcp::endpoint endpoint)
      : socket_(io_ctx), peer_(peer), peer_id_(peer_id), endpoint_(endpoint),
        connected_(false) {}

  ~ClientSession() {}

  // Send data over the connection
  void send(uint8_t transport_type, const uint8_t *data, uint32_t len) {
    TransportMeta meta;
    meta.type = transport_type;
    meta.len = htonl(len);
  }

  // Start async connection to remote peer
  void start_connect() {
    // TODO: We'll implement this next
  }

  // Close the session
  void close_session() {
    // TODO: We'll implement this next
  }

private:
  boost::asio::ip::tcp::socket socket_;
  boost::asio::ip::tcp::endpoint endpoint_;
  PeerImpl *peer_;
  uint64_t peer_id_;
  std::vector<uint8_t> buffer_;
  bool connected_;
};

// PeerImpl implements the Peer interface
class PeerImpl : public Peer {
public:
  PeerImpl(boost::asio::io_context &io_ctx, uint64_t peer_id,
           const std::string &peer_addr)
      : peer_id_(peer_id), io_ctx_(io_ctx) {

    // Parse "127.0.0.1:5001" into IP and port
    size_t colon_pos = peer_addr.find(':');
    if (colon_pos == std::string::npos) {
      std::cerr << "Invalid address format: " << peer_addr << std::endl;
      exit(1);
    }

    std::string ip = peer_addr.substr(0, colon_pos);
    std::string port_str = peer_addr.substr(colon_pos + 1);
    int port = std::stoi(port_str);

    // Create endpoint (IP + port)
    auto address = boost::asio::ip::make_address(ip);
    endpoint_ = boost::asio::ip::tcp::endpoint(address, port);
  }

  ~PeerImpl() override {}

  void start() override {
    // TODO: We'll implement this
  }

  void send(proto::MessagePtr msg) override {
    // TODO: Send serialized message to the peer we connected to
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, *msg);
    do_send_data(TransportTypeStream, (const uint8_t *)sbuf.data(),
                 (uint32_t)sbuf.size());
  }

  void stop() override {
    // TODO: We'll implement this
  }

private:
  void do_send_data(uint8_t type, const uint8_t *data, uint32_t len);

  uint64_t peer_id_;
  boost::asio::io_context &io_ctx_;
  boost::asio::ip::tcp::endpoint endpoint_;
  std::shared_ptr<ClientSession> session_;
};

void PeerImpl::do_send_data(uint8_t type, const uint8_t *data, uint32_t len) {
  if (!session_) {
    session_ =
        std::make_shared<ClientSession>(io_ctx_, this, peer_id_, endpoint_);
    session_->send(type, data, len);
    session_->start_connect();
  } else {
    session_->send(type, data, len);
  }
}

std::shared_ptr<Peer>
Peer::create(uint64_t peer_id, const std::string &peer_addr, void *io_service) {

  auto &io_ctx = *(boost::asio::io_context *)io_service;
  return std::make_shared<PeerImpl>(io_ctx, peer_id, peer_addr);
}

} // namespace kv
