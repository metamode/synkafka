#pragma once

#include <deque>

#include "message_set.h"
#include "packet.h"
#include "slice.h"

namespace synkafka {
namespace proto {

struct RequestHeader
{
	int16_t 	api_key;
	int16_t 	api_version;
	int32_t 	correlation_id;
	slice	    client_id;
};

inline void kafka_proto_io(PacketCodec& p, RequestHeader& h)
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

inline void kafka_proto_io(PacketCodec& p, ResponseHeader& h)
{
	p.io(h.correlation_id);
}


struct TopicMetadataRequest
{
	static const int16_t api_key = ApiKey::MetadataRequest;

	std::deque<std::string> topic_names;
};

inline void kafka_proto_io(PacketCodec& p, TopicMetadataRequest& r)
{
	p.io(r.topic_names);
}


struct Broker
{
	int32_t 	node_id;
	std::string host;
	int32_t 	port;
};

inline void kafka_proto_io(PacketCodec& p, Broker& b)
{
	p.io(b.node_id);
	p.io(b.host);
	p.io(b.port);
}


struct PartitionMetaData
{
	std::error_code	err_code;
	int32_t 	partition_id;
	int32_t 	leader;
	std::deque<int32_t> replicas;
	// In Sync Replica set
	std::deque<int32_t> isr;
};

inline void kafka_proto_io(PacketCodec& p, PartitionMetaData& pmd)
{
	p.io(pmd.err_code);
	p.io(pmd.partition_id);
	p.io(pmd.leader);
	p.io(pmd.replicas);
	p.io(pmd.isr);
}


struct TopicMetaData
{
	std::error_code						err_code;
	std::string 					name;
	std::deque<PartitionMetaData> 	partitions;
};

inline void kafka_proto_io(PacketCodec& p, TopicMetaData& tmd)
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

inline void kafka_proto_io(PacketCodec& p, MetadataResponse& r)
{
	p.io(r.brokers);
	p.io(r.topics);
}


struct ProducePartition
{
	int32_t 	partition_id;
	MessageSet  messages;
};

inline void kafka_proto_io(PacketCodec& p, ProducePartition& pms)
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
		kafka_proto_io_impl(p, pms.messages, message_set_len);
	}
}

struct ProduceTopic
{
	std::string 					name;
	std::deque<ProducePartition> 	partitions;
};

inline void kafka_proto_io(PacketCodec& p, ProduceTopic& pb)
{
	p.io(pb.name);
	p.io(pb.partitions);
}


struct ProduceRequest
{
	static const int16_t api_key = ApiKey::ProduceRequest;

	int16_t 					required_acks;
	int32_t 					timeout;
	std::deque<ProduceTopic> 	topics;
};

inline void kafka_proto_io(PacketCodec& p, ProduceRequest& r)
{
	p.io(r.required_acks);
	p.io(r.timeout);
	p.io(r.topics);
}


struct ProduceResponsePartition
{
	int32_t 	partition_id;
	std::error_code	err_code;
	int64_t 	offset;
};

inline void kafka_proto_io(PacketCodec& p, ProduceResponsePartition& rp)
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

inline void kafka_proto_io(PacketCodec& p, ProduceResponseTopic& rt)
{
	p.io(rt.name);
	p.io(rt.partitions);
}


struct ProduceResponse
{
	std::deque<ProduceResponseTopic> topics;
};

inline void kafka_proto_io(PacketCodec& p, ProduceResponse& r)
{
	p.io(r.topics);
}

}}