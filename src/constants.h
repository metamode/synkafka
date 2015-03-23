#pragma once

#include "errors.h"

namespace synkafka {

enum CompressionType
{
	COMP_None   = 0,
	COMP_GZIP   = 1,
	COMP_Snappy = 2,
};

// Kafka protocol "API keys" i.e. RPC method ids
// Not enum because we have to encode/decode from int16_t on wire and it's just tedious
// to manually cast all the time. Type safety is not so critical here.
namespace ApiKey
{
	const int16_t ProduceRequest 			= 0;
	const int16_t FetchRequest 				= 1;
	const int16_t OffsetRequest 			= 2;
	const int16_t MetadataRequest 			= 3;
	// 4 - 7 reserved for internal Kafka APIs
	const int16_t OffsetCommitRequest		= 8;
	const int16_t OffsetFetchRequest      	= 9;
	const int16_t ConsumerMetadataRequest 	= 10;
};

// Api version sent in all request headers for 0.8.x
const int16_t KafkaApiVersion = 0;

}