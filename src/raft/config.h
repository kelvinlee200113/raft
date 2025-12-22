#pragma once
#include <stdint.h>
#include <vector>

namespace kv {
    
struct Config {
    uint64_t id;
    std::vector<uint64_t> peers;
    uint32_t election_tick;
    uint32_t heartbeat_tick;

    Config() : id(0), election_tick(10), heartbeat_tick(1) {}
};




}
