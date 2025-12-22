#pragma once
#include <stdint.h>
#include <vector>

namespace kv {
  
namespace proto {

typedef uint8_t EntryType;
const EntryType EntryNormal = 0;
const EntryType EntryConfChange = 1;

struct Entry {
    EntryType type;
    uint64_t term;
    uint64_t index;    
    std::vector<uint8_t> data;

    Entry(): type(EntryNormal), term(0), index(0) {}
};









}
}