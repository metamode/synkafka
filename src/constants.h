#pragma once

enum CompressionType
{
	COMP_None   = 0,
	COMP_GZIP   = 1,
	COMP_Snappy = 2,
};

// Kafka protocol error codes
enum ErrorCode
{
	E_NoError 								= 0, // No error--it worked!
	E_Unknown 								= -1, // An unexpected server error
	E_OffsetOutOfRange 						= 1, // The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
	E_InvalidMessage 						= 2, // This indicates that a message contents does not match its CRC
	E_UnknownTopicOrPartition 				= 3, // This request is for a topic or partition that does not exist on this broker.
	E_InvalidMessageSize 					= 4, // The message has a negative size
	E_LeaderNotAvailable 					= 5, // This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
	E_NotLeaderForPartition 				= 6, // This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
	E_RequestTimedOut 						= 7, // This error is thrown if the request exceeds the user-specified time limit in the request.
	E_BrokerNotAvailable 					= 8, // This is not a client facing error and is used mostly by tools when a broker is not alive.
	E_ReplicaNotAvailable 					= 9, // If replica is expected on a broker, but is not (this can be safely ignored).
	E_MessageSizeTooLarge 					= 10, // The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
	E_StaleControllerEpochCode 				= 11, // Internal error code for broker-to-broker communication.
	E_OffsetMetadataTooLargeCode 			= 12, // If you specify a string larger than configured maximum for offset metadata
	E_OffsetsLoadInProgressCode 			= 14, // The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
	E_ConsumerCoordinatorNotAvailableCode 	= 15, 	// The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
	E_NotCoordinatorForConsumerCode 		= 16, // The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
};

// Kafka protocol "API keys" i.e. RPC method ids
enum ApiKey
{
	API_ProduceRequest 			= 0,
	API_FetchRequest 			= 1,
	API_OffsetRequest 			= 2,
	API_MetadataRequest 		= 3,
	// 4 - 7 reserved for internal Kafka APIs
	API_OffsetCommitRequest		= 8,
	API_OffsetFetchRequest      = 9,
	API_ConsumerMetadataRequest = 10,
};