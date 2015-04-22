#pragma once

#include <string>
#include <system_error>

#include "constants.h"

namespace synkafka {


// Kafka protocol error codes
// CamelCase is not very system::error_code-like but it preserves direct
// equivalence with Kafka docs which is somewhat useful
// Rest of error_code implementation in errors.h
enum class kafka_error
{
    NoError                             = 0,  // No error--it worked!
    Unknown                             = -1, // An unexpected server error
    OffsetOutOfRange                     = 1,  // The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
    InvalidMessage                         = 2,  // This indicates that a message contents does not match its CRC
    UnknownTopicOrPartition             = 3,  // This request is for a topic or partition that does not exist on this broker.
    InvalidMessageSize                     = 4,  // The message has a negative size
    LeaderNotAvailable                     = 5,  // This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
    NotLeaderForPartition                 = 6,  // This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
    RequestTimedOut                     = 7,  // This error is thrown if the request exceeds the user-specified time limit in the request.
    BrokerNotAvailable                     = 8,  // This is not a client facing error and is used mostly by tools when a broker is not alive.
    ReplicaNotAvailable                 = 9,  // If replica is expected on a broker, but is not (this can be safely ignored).
    MessageSizeTooLarge                 = 10, // The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
    StaleControllerEpochCode             = 11, // Internal error code for broker-to-broker communication.
    OffsetMetadataTooLargeCode             = 12, // If you specify a string larger than configured maximum for offset metadata
    OffsetsLoadInProgressCode             = 14, // The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
    ConsumerCoordinatorNotAvailableCode = 15, // The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
    NotCoordinatorForConsumerCode         = 16, // The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
};

class kafka_category_impl
    : public std::error_category
{
public:
    const char* name() const noexcept { return "kafka"; }

    std::string message(int ev) const
    {
        switch(static_cast<kafka_error>(ev))
        {
        case kafka_error::NoError:
            return "No error--it worked!";
        case kafka_error::Unknown:
            return "An unexpected server error";
        case kafka_error::OffsetOutOfRange:
            return "The requested offset is outside the range of offsets maintained by the server for the given topic/partition.";
        case kafka_error::InvalidMessage:
            return "This indicates that a message contents does not match its CRC";
        case kafka_error::UnknownTopicOrPartition:
            return "This request is for a topic or partition that does not exist on this broker.";
        case kafka_error::InvalidMessageSize:
            return "The message has a negative size";
        case kafka_error::LeaderNotAvailable:
            return "This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.";
        case kafka_error::NotLeaderForPartition:
            return "This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.";
        case kafka_error::RequestTimedOut:
            return "This error is thrown if the request exceeds the user-specified time limit in the request.";
        case kafka_error::BrokerNotAvailable:
            return "This is not a client facing error and is used mostly by tools when a broker is not alive.";
        case kafka_error::ReplicaNotAvailable:
            return "If replica is expected on a broker, but is not (this can be safely ignored).";
        case kafka_error::MessageSizeTooLarge:
            return "The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.";
        case kafka_error::StaleControllerEpochCode:
            return "Internal error code for broker-to-broker communication.";
        case kafka_error::OffsetMetadataTooLargeCode:
            return "If you specify a string larger than configured maximum for offset metadata";
        case kafka_error::OffsetsLoadInProgressCode:
            return "The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).";
        case kafka_error::ConsumerCoordinatorNotAvailableCode:
            return "The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.";
        case kafka_error::NotCoordinatorForConsumerCode:
            return "The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.";
        default:
            return "Invalid Kafka Error";
        }
    }

    std::error_condition default_error_condition(int ev) const noexcept
    {
        switch (static_cast<kafka_error>(ev))
          {
          case kafka_error::RequestTimedOut:
            return std::errc::stream_timeout;
          case kafka_error::InvalidMessage:
            return std::errc::bad_message;
          case kafka_error::InvalidMessageSize:
            return std::errc::bad_message;
          case kafka_error::MessageSizeTooLarge:
            return std::errc::message_size;
          default:
            return std::error_condition(ev, *this);
          }
    }
};

inline const std::error_category& kafka_category()
{
    static kafka_category_impl instance;
      return instance;
}

inline std::error_code make_error_code(kafka_error e)
{
  return std::error_code(static_cast<int>(e), kafka_category());
}

inline std::error_condition make_error_condition(kafka_error e)
{
  return std::error_condition(static_cast<int>(e), kafka_category());
}

}

namespace std
{
    template <>
    struct is_error_code_enum<synkafka::kafka_error>
          : public true_type {};
}