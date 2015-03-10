#pragma once

#include <deque>

#include "packet.h"

namespace synkafka {
namespace proto {

struct RequestHeader
{
	int16_t 	api_key;
	int16_t 	api_version;
	int32_t 	correlation_id;
	std::string client_id;
};

void kafka_proto_io(PacketCodec& p, RequestHeader& h)
{
	p.io(h.api_key);
	p.io(h.api_version);
	p.io(h.correlation_id);
	p.io(h.client_id);
}


struct ResponseHeader
{
	int32_t correlation_id;
};

void kafka_proto_io(PacketCodec& p, ResponseHeader& h)
{
	p.io(h.correlation_id);
}


struct Message
{
	CompressionType  	compression;
	std::string			key;
	std::string			value;
};

void kafka_proto_io(PacketCodec& p, Message& m)
{
	// 0.8.x protocol has 0 magic byte
	int8_t magic = 0;

	// Need to encode attributes
	int8_t attributes = 0;

	if (p.is_writer()) {
		// Attributes only contains compression in lowest 2 bits in 0.8.x
		attributes = static_cast<int8_t>(m.compression) & 0x3;
	}

	auto crc = p.start_crc();
	p.io(magic);
	p.io(attributes);

	if (!p.is_writer()) {
		// Attributes only contains compression in lowest 2 bits in 0.8.x
		m.compression = static_cast<CompressionType>(attributes & 0x3);
	}

	p.io_bytes(m.key, COMP_None);
	p.io_bytes(m.value, m.compression);
	p.end_crc(crc);
}


struct MessageSet
{
	CompressionType  						compression;
	std::deque<std::pair<int64_t, Message>> messages;
};

void kafka_proto_io(PacketCodec& p, MessageSet& ms, int32_t encoded_length = -1)
{
	if (p.is_writer()) {
		if (ms.compression == COMP_None) {
			for (auto& pair : ms.messages) {
				// Encode offset
				p.io(pair.first);
				// Add encoded message length field
				auto len_field = p.start_length();
				// Encode message
				p.io(pair.second);
				// Update length field
				p.end_length(len_field);
			}
		} else {
			// First we need to encode into an uncompressed messages set in a temp buffer
			// Use heuristic to minimise re-allocations, assume each message turns out to be around 1k
			// We could actually measure this if it turns out to be a performance bottleneck...
			PacketEncoder pe(1024 * ms.messages.size());

			// Store the actual compression requested and make a recursive call with no compression set
			auto actual_compression = ms.compression;
			ms.compression = COMP_None;

			pe.io(ms);

			if (!pe.ok()) {
				p.set_err(pe.err())
					<< "Failed to encoded message set before compression: "
					<< pe.err_str();
				return;
			}

			// Now encode a regular message with that value.
			// Note that for now this means a whole extra copy since we need to assign the encoded buffer to a string
			// Maybe if we ever need to optimise this we can find a cleaner way to allow non-copy references in message value
			// without making them much harder to work with in general.

			// Finally we add the new message to a new singleton message set and encode that.
			// Note this one has no compression marked since we already compressed and the flag
			// is in the nested message.
			MessageSet compressed_ms{COMP_None, {std::make_pair(0, proto::Message{actual_compression, "", std::move(pe.get_as_slice(false).str())})}};

			// Now we can encode the message set with single compressed message into the output buffer
			p.io(compressed_ms);
		}
	} else {
		// No length prefix for messages and we might have partial one at end of buffer legitimately
		// Keep reading until we have them all (or hit error)...
		// In some cases a MessageSet might be embedded inside a larger Structure with a length prefix before it
		// in this case we need to stop reading once we have consumed the length the prefix gave otherwise we might
		// read past the end and into non MessageSet bytes that follow.
		// Decoder provides access to the last length prefix passed for this purpose, although it is not always present
		// so only use it if it is
		auto start_offset = p.get_cursor();

		while (p.ok() && (encoded_length == -1 || (p.get_cursor() - start_offset) < encoded_length)) {
			int64_t offset;
			Message m;

			p.io(offset);
			auto len_field = p.start_length();
			p.io(m);
			p.end_length(len_field);

			if (p.ok()) {
				if (m.compression == COMP_None) {
					ms.messages.emplace_back(offset, std::move(m));
				} else {
					// Message decoder already should have detected compression and decompressed the message's value
					// we just need to decode that as a nested message set and append messages to this message set...
					// Note that this means a whole 2 extra copies currently - we decompressed into a buffer, copied to a string
					// and now copy string back to buffer again. Will fix it if performance becomes important.
					PacketDecoder pd(buffer_from_string(m.value));

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


struct TopicMetadataRequest
{
	std::deque<std::string> topic_names;
};

void kafka_proto_io(PacketCodec& p, TopicMetadataRequest& r)
{
	p.io(r.topic_names);
}


struct Broker
{
	int32_t 	node_id;
	std::string host;
	int32_t 	port;
};

void kafka_proto_io(PacketCodec& p, Broker& b)
{
	p.io(b.node_id);
	p.io(b.host);
	p.io(b.port);
}


struct PartitionMetaData
{
	int16_t		err_code;
	int32_t 	partition_id;
	int32_t 	leader;
	std::deque<int32_t> replicas;
	// In Sync Replica set
	std::deque<int32_t> isr;
};

void kafka_proto_io(PacketCodec& p, PartitionMetaData& pmd)
{
	p.io(pmd.err_code);
	p.io(pmd.partition_id);
	p.io(pmd.leader);
	p.io(pmd.replicas);
	p.io(pmd.isr);
}


struct TopicMetaData
{
	int16_t 	err_code;
	std::string name;
	std::deque<PartitionMetaData> partitions;
};

void kafka_proto_io(PacketCodec& p, TopicMetaData& tmd)
{
	p.io(tmd.err_code);
	p.io(tmd.name);
	p.io(tmd.partitions);
}


struct MetadataResponse
{
	std::deque<Broker> 			brokers;
	std::deque<TopicMetaData> 	topics;
};

void kafka_proto_io(PacketCodec& p, MetadataResponse& r)
{
	p.io(r.brokers);
	p.io(r.topics);
}


struct ProducePartition
{
	int32_t 	partition_id;
	MessageSet  messages;
};


void kafka_proto_io(PacketCodec& p, ProducePartition& pms)
{
	p.io(pms.partition_id);

	if (p.is_writer()) {
		auto len_field = p.start_length();
		p.io(pms.messages);
		p.end_length(len_field);
	} else {
		// Slight hack - when reading we need to pass the length
		// of message set along to avoid trying to decode more parts of the message
		// as part of the message set.
		int32_t message_set_len = 0;
		p.io(message_set_len);
		kafka_proto_io(p, pms.messages, message_set_len);
	}
}

struct ProduceTopic
{
	std::string 					name;
	std::deque<ProducePartition> 	partitions;
};

void kafka_proto_io(PacketCodec& p, ProduceTopic& pb)
{
	p.io(pb.name);
	p.io(pb.partitions);
}


struct ProduceRequest
{
	int16_t 					required_acks;
	int32_t 					timeout;
	std::deque<ProduceTopic> 	topics;
};

void kafka_proto_io(PacketCodec& p, ProduceRequest& r)
{
	p.io(r.required_acks);
	p.io(r.timeout);
	p.io(r.topics);
}


struct ProduceResponsePartition
{
	int32_t 	partition_id;
	int16_t 	err_code;
	int64_t 	offset;
};

void kafka_proto_io(PacketCodec& p, ProduceResponsePartition& rp)
{
	p.io(rp.partition_id);
	p.io(rp.err_code);
	p.io(rp.offset);
}


struct ProduceResponseTopic
{
	std::string 							name;
	std::deque<ProduceResponsePartition> 	partitions;
};

void kafka_proto_io(PacketCodec& p, ProduceResponseTopic& rt)
{
	p.io(rt.name);
	p.io(rt.partitions);
}


struct ProduceResponse
{
	std::deque<ProduceResponseTopic> topics;
};

void kafka_proto_io(PacketCodec& p, ProduceResponse& r)
{
	p.io(r.topics);
}

}}