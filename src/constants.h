#pragma once

namespace synkafka {

enum CompressionType
{
	COMP_None   = 0,
	COMP_GZIP   = 1,
	COMP_Snappy = 2,
};

// Not enum because we have to encode/decode from int16_t on wire and it's just tedious
// to manually cast all the time. Type safety is not so critical here.

// Kafka protocol error codes
namespace KafkaErrCode
{
	const int16_t NoError 								= 0;  // No error--it worked!
	const int16_t Unknown 								= -1; // An unexpected server error
	const int16_t OffsetOutOfRange 						= 1;  // The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
	const int16_t InvalidMessage 						= 2;  // This indicates that a message contents does not match its CRC
	const int16_t UnknownTopicOrPartition 				= 3;  // This request is for a topic or partition that does not exist on this broker.
	const int16_t InvalidMessageSize 					= 4;  // The message has a negative size
	const int16_t LeaderNotAvailable 					= 5;  // This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
	const int16_t NotLeaderForPartition 				= 6;  // This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
	const int16_t RequestTimedOut 						= 7;  // This error is thrown if the request exceeds the user-specified time limit in the request.
	const int16_t BrokerNotAvailable 					= 8;  // This is not a client facing error and is used mostly by tools when a broker is not alive.
	const int16_t ReplicaNotAvailable 					= 9;  // If replica is expected on a broker, but is not (this can be safely ignored).
	const int16_t MessageSizeTooLarge 					= 10; // The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
	const int16_t StaleControllerEpochCode 				= 11; // Internal error code for broker-to-broker communication.
	const int16_t OffsetMetadataTooLargeCode 			= 12; // If you specify a string larger than configured maximum for offset metadata
	const int16_t OffsetsLoadInProgressCode 			= 14; // The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
	const int16_t ConsumerCoordinatorNotAvailableCode 	= 15; // The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
	const int16_t NotCoordinatorForConsumerCode 		= 16; // The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
};

// Kafka protocol "API keys" i.e. RPC method ids
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

}