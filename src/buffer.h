#pragma once

#include <memory>
#include <string>
#include <vector>

namespace synkafka {

typedef std::vector<uint8_t> buffer_t;
typedef std::shared_ptr<buffer_t> shared_buffer_t;

inline shared_buffer_t buffer_from_string(const std::string& str)
{
    shared_buffer_t b = std::make_shared<buffer_t>(str.begin(), str.end());
    return b;
}

inline shared_buffer_t buffer_from_string(const char* c_str, size_t len)
{
    std::string str(c_str, len);
    return buffer_from_string(str);
}

inline shared_buffer_t make_shared_buffer(size_t initial_size)
{
    return std::make_shared<buffer_t>(initial_size);
}

}