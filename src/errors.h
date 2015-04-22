#pragma once

#include <string>
#include <system_error>

#include "constants.h"
#include "kafka_errors.h"

namespace synkafka {


// Kafka protocol error codes
// CamelCase is not very system::error_code-like but it preserves direct
// equivalence with Kafka docs which is somewhat useful
// Rest of error_code implementation in errors.h
enum class synkafka_error
{
    no_error = 0,
    bad_config,
    network_fail,
    network_timeout,
    message_set_full,
    compression_lib_error,
    client_stopping,
    encoding_error,
    decoding_error,
    unknown,
};

class synkafka_error_category_impl
    : public std::error_category
{
public:
    const char* name() const noexcept { return "synkafka"; }

    std::string message(int ev) const
    {
        switch(static_cast<synkafka_error>(ev))
        {
        case synkafka_error::no_error:
            return "OK";
        case synkafka_error::bad_config:
            return "Bad Skykafka configuration";
        case synkafka_error::network_fail:
            return "TCP connection failed, or host not available";
        case synkafka_error::network_timeout:
            return "TCP connection timed out";
        case synkafka_error::message_set_full:
            return "MessageSet is full - adding that item would make it's encoded size potentially bigger than configured maximum";
        case synkafka_error::compression_lib_error:
            return "Error (de)compressing buffer from compression library";
        case synkafka_error::client_stopping:
            return "Client has shut down";
        case synkafka_error::encoding_error:
            return "Error encoding protocol bytes";
        case synkafka_error::decoding_error:
            return "Error decoding protocol bytes";
        case synkafka_error::unknown:
            return "Unknown error";
        default:
            return "Invalid Synkafka Error";
        }
    }

    std::error_condition default_error_condition(int ev) const noexcept
    {
        switch (static_cast<synkafka_error>(ev))
          {
          case synkafka_error::bad_config:
              return std::errc::invalid_argument;
          case synkafka_error::network_timeout:
            return std::errc::stream_timeout;
          default:
            return std::error_condition(ev, *this);
          }
    }
};

inline const std::error_category& synkafka_category()
{
    static synkafka_error_category_impl instance;
      return instance;
}

inline std::error_code make_error_code(synkafka_error e)
{
  return std::error_code(static_cast<int>(e), synkafka_category());
}

inline std::error_condition make_error_condition(synkafka_error e)
{
  return std::error_condition(static_cast<int>(e), synkafka_category());
}

}

namespace std
{
    template <>
    struct is_error_code_enum<synkafka::synkafka_error>
          : public true_type {};
}