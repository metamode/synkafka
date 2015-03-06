#pragma once

#include <memory>
#include <vector>

namespace synkafka {

typedef std::vector<uint8_t> buffer_t;
typedef std::shared_ptr<buffer_t> shared_buffer_t;

}