#pragma once

#include <deque>
#include <string>
#include <system_error>

#include "slice.h"

namespace synkafka {

class ProducerClient
{
public:

	// Start a client with list of known broker hostname:port strings
	// If you miss out port we assume default 9092
	// Most uses probaly only need one background io thread for networking but increasing it
	// *might* improve latency/throughput in some situations
	ProducerClient(const std::deque<std::string>& brokers, int num_io_threads = 1);

	// Set the timeout in milliseconds sent to kafka to wait for acks.
	// Default is 10 seconds
	void set_produce_timeout(int32_t milliseconds);

	// Set timeout in milliseconds used when trying to connect to a broker.
	//Default is 10 seconds
	void set_connect_timeout(int32_t milliseconds);

	// Set timeout in milliseconts used to allow for network RTT on top of
	// produce_timeout. This is necessary because Kafka will wait for minimum
	// of produce_timeout for required acks before responding so we need to allow
	// additional time to allow for network traffic or slight delays in kafka due to overload
	// e.t.c. default is 1 second
	void set_produce_timeout_rtt_allowance(int32_t milliseconds);
	void set_required_acks(int32_t acks);

	// Check if we have a known leader and are able to connect to it
	// for a given topic partition.
	// Note that this may attempt to connect to broker if we have no connection and so may
	// block for up to the connect_timeout.
	// Non-error return value of 0 means we are all good, otherwise assume not available and inspect category and code for reason.
	// category may be synkafka, kafka or system... std::errc conditions are defined for cases like timeout and invalid config.
	std::error_code check_topic_partition_leader_available(const slice& topic, int32_t partition_id);


	// Synchronously produce a batch of messages
	// We assume the messages were already bult using the MessageSet class which validates
	// for known issues like maximum message size.
	// The caller must be configured to set max_message_size on the MessageSet correctly...
	// The returned error_code
	std::error_code produce(const slice& topic, int32_t partition_id, MessageSet& messages);

};
}