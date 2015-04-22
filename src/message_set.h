#pragma once

#include <deque>
#include <list>

#include "buffer.h"
#include "constants.h"
#include "packet.h"
#include "slice.h"

namespace synkafka {


class MessageSet
{
public:
    struct Message
    {
        slice             key;
        slice             value;
        int64_t         offset;
        // Only used internally when reading messages
        CompressionType compression_;
    };

    MessageSet();

    void set_compression(CompressionType comp);

    // Set the max message size as configured with Kafka for this topic
    // You must have your config in sync between Kafka and here - max message size
    // is not reported in topic metadata and may result in messages being reject if you
    // set a higher value. Kafka's defaut is 1000000 bytes so that is our default here too.
    void set_max_message_size(size_t max_message_size);

    // Push another message onto the set, this will check the message for simple things that
    // will get it rejected. If returned error code is non-zero then the message is NOT added
    // to set and can't be given current configuration.
    // If returned error code has category synkafka and value of synkafka_error::message_set_full
    // then no further pushes will succeed as message bytes would exceed Kafka's max_message_bytes
    // and be rejected.
    // For compressed batches this is potentially sub-optimal but since we can't know until we compressed
    // the whole set how large the compressed message will be, we must be conservative and assume that
    // worst case compression and headers.
    // If copy = true, a copy of value and key is made to an internal buffer so the caller doesn't need
    // to keep slices valid. If if's omitted or set to false explicitly then caller MUST ensure data backing
    // slices remains valid until MessageSet is destroyed (or produce() call returns if passed to that).
    std::error_code push(const slice& message, const slice& key, bool copy = false);
    std::error_code push(Message&& m);

    // Allow encode/decode like the primative structs, by the time we get to actually encode
    // it is REQUIRED that the MessageSet is in a valid state (i.e. non empty and not too big).
    // Note friend can't have optional length so this is an implementation function that is called by
    // kafka_proto_io
    friend void kafka_proto_io_impl(PacketCodec& p, MessageSet& ms, int32_t encoded_length);

    const std::deque<Message>& get_messages() const { return messages_; }
    size_t get_encoded_size() const { return encoded_size_; }

private:
    size_t get_msg_encoded_size(const Message& m) const;
    size_t get_worst_case_compressed_size(size_t size) const;

    void message_io(PacketCodec& p, Message& m, CompressionType& compression);

    std::deque<Message>     messages_;
    size_t                    max_message_size_;
    CompressionType          compression_;
    size_t                    encoded_size_;

    // Any strings we need to keep around to keep slices valid
    std::list<buffer_t>        owned_buffers_;
};

void kafka_proto_io(PacketCodec& p, MessageSet::Message& m);

void kafka_proto_io(PacketCodec& p, MessageSet& ms);

}