#include <snappy.h>
#include <zlib.h>

#include "message_set.h"

namespace synkafka {

MessageSet::MessageSet()
    : messages_()
    , max_message_size_(1000000) // Kafka default
    , compression_(COMP_None)
    , encoded_size_(0)
    , owned_buffers_()
{}

void MessageSet::set_compression(CompressionType comp)
{
    compression_ = comp;
}

void MessageSet::set_max_message_size(size_t max_message_size)
{
    max_message_size_ = max_message_size;
}

std::error_code MessageSet::push(const slice& message, const slice& key, bool copy)
{
    if (copy) {
        // Copy slices into an internal buffer to "save" them
        buffer_t buff(key.size() + message.size());

        std::memcpy(&buff[0], key.data(), key.size());
        std::memcpy(&buff[key.size()], message.data(), message.size());

        Message m{slice(&buff[0], key.size())
                 ,slice(&buff[key.size()], message.size())
                 };

        owned_buffers_.push_back(std::move(buff));

        return push(std::move(m));
    }

    Message m{key, message};
    return push(std::move(m));
}

std::error_code MessageSet::push(Message&& m)
{
    auto encoded_size = get_msg_encoded_size(m);

    if (get_worst_case_compressed_size(encoded_size_ + encoded_size) > max_message_size_) {
        return make_error_code(synkafka_error::message_set_full);
    }

    // Not exception safe...
    encoded_size_ += encoded_size;
    messages_.push_back(std::move(m));

    return make_error_code(synkafka_error::no_error);
}

size_t MessageSet::get_msg_encoded_size(const Message& m) const
{
    // 0.8.x protocol...
    return sizeof(int64_t) // Message offset
        + sizeof(int32_t)    // Message length
        // Message itself
        + sizeof(int32_t)    // CRC32
        + sizeof(int8_t)       // Magic byte
        + sizeof(int8_t)      // Attributes
        + sizeof(int32_t)     // Key length prefix
        + m.key.size()
        + sizeof(int32_t)     // Value length prefix
        + m.value.size();

}

size_t MessageSet::get_worst_case_compressed_size(size_t size) const
{
    switch (compression_)
    {
    case COMP_None:
        return size;
    case COMP_GZIP:
    {
        z_stream strm;

        // Initialize gzip compression
        memset(&strm, 0, sizeof(strm));
        auto r = deflateInit2(&strm, Z_DEFAULT_COMPRESSION,
                 Z_DEFLATED, 15+16, // 15+16 forces zlib to add the gzip header and trailer
                 8, Z_DEFAULT_STRATEGY);

        if (r != Z_OK) {
            // Errr... not sure what else we can do in here
            throw make_error_code(synkafka_error::compression_lib_error);
        }
        auto worst_case = deflateBound(&strm, size);

        // Free memory
        deflateEnd(&strm);

        return worst_case;
    }
    case COMP_Snappy:
        return snappy::MaxCompressedLength(size);
    }

    // GCC is apparently not smart enough to realise we implemented case for every defined value of enum above
    // this is unreachable but to save the "control reaches end of non-void function" warning...
    return 0;
}

void kafka_proto_io(PacketCodec& p, MessageSet::Message& m)
{
    // 0.8.x protocol has 0 magic byte
    int8_t magic = 0;

    // Need to encode attributes
    int8_t attributes = 0;

    if (p.is_writer()) {
        // Attributes only contains compression in lowest 2 bits in 0.8.x
        attributes = static_cast<int8_t>(m.compression_) & 0x3;
    }

    auto crc = p.start_crc();
    p.io(magic);
    p.io(attributes);

    if (!p.is_writer()) {
        // Attributes only contains compression in lowest 2 bits in 0.8.x
        m.compression_ = static_cast<CompressionType>(attributes & 0x3);
    }

    p.io_bytes(m.key, COMP_None);
    p.io_bytes(m.value, m.compression_);
    p.end_crc(crc);
}

void kafka_proto_io(PacketCodec& p, MessageSet& ms)
{
    kafka_proto_io_impl(p, ms, -1);
}

// Implementation of encoding/decoding with length argument that is not provided with normal Decoder/Encoder calls
void kafka_proto_io_impl(PacketCodec& p, MessageSet& ms, int32_t encoded_length)
{
    if (p.is_writer()) {
        if (ms.compression_ == COMP_None) {
            for (auto& message : ms.messages_) {
                // Encode offset
                p.io(message.offset);
                // Add encoded message length field
                auto len_field = p.start_length();
                // Encode message
                p.io(message);
                // Update length field
                p.end_length(len_field);
            }
        } else {
            // First we need to encode into an uncompressed messages set in a temp buffer
            PacketEncoder pe(ms.encoded_size_);

            // Store the actual compression requested and make a recursive call with no compression set
            auto actual_compression = ms.compression_;
            ms.compression_ = COMP_None;

            pe.io(ms);

            if (!pe.ok()) {
                p.set_err(pe.err())
                    << "Failed to encoded message set before compression: "
                    << pe.err_str();
                return;
            }

            // Reset compression to avoid confusion
            ms.compression_ = actual_compression;

            // Now encode a regular message with that value.
            // Note that for now this means a whole extra copy since we need to assign the encoded buffer to a string
            // Maybe if we ever need to optimise this we can find a cleaner way to allow non-copy references in message value
            // without making them much harder to work with in general.
            MessageSet::Message m{slice(), pe.get_as_slice(false), 0, actual_compression};

            // Now we can encode the message set with single compressed message into the output buffer
            // Encode null offset
            static int64_t zero = 0;
            p.io(zero);
            // Add encoded message length field
            auto len_field = p.start_length();
            // Encode message
            p.io(m);
            // Update length field
            p.end_length(len_field);
        }
    } else {
        // No length prefix for messages and we might have partial one at end of buffer legitimately
        // Keep reading until we have them all (or hit error)...
        // In some cases a MessageSet might be embedded inside a larger structure with a length prefix before it
        // in this case we need to stop reading once we have consumed the length the prefix gave otherwise we might
        // read past the end and into non MessageSet bytes that follow.
        // Decoder provides access to the last length prefix passed for this purpose, although it is not always present
        // so only use it if it is
        auto start_offset = p.get_cursor();

        while (p.ok() && (encoded_length == -1 || (p.get_cursor() - start_offset) < static_cast<size_t>(encoded_length))) {
            MessageSet::Message m;

            p.io(m.offset);
            auto len_field = p.start_length();
            p.io(m);
            p.end_length(len_field);

            if (p.ok()) {
                if (m.compression_ == COMP_None) {
                    // This doesn't
                    ms.push(std::move(m));
                } else {
                    // Message decoder already should have detected compression and decompressed the message's value
                    // we just need to decode that as a nested message set and append messages to this message set...
                    // We rely on the decompressed buffer being the last one decompressed and so it is on the end of the
                    // Decoder's list. We get it and use directly which both saves copy overhead but more importantly
                    // means the messages we decode will have their key/value slices pointing into a valid buffer once
                    // this PacketDecoder is destructed below.
                    auto decoder = reinterpret_cast<PacketDecoder *>(&p);
                    PacketDecoder pd(decoder->get_last_decompress_buffer());

                    // Decode messages directly into the message set here - new ones will be appended.
                    pd.io(ms);
                }
            }
        }

        // Not having enough bytes to read more is a normal termination condition for reads regardless of if we had
        // partial message at end off buffer or not. In both cases it's expected case and so should not be left as
        // an error state. Any other error should be left though.
        if (p.err() == PacketCodec::ERR_TRUNCATED) {
            p.set_err(PacketCodec::ERR_NONE);
        }
    }
}

}