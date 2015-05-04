#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <string>
#include <system_error>
#include <memory>
#include <mutex>
#include <thread>
#include <map>

#include <boost/asio.hpp>
#include <boost/core/noncopyable.hpp>

#include "broker.h"
#include "protocol.h"
#include "slice.h"

namespace synkafka {

class ProducerClient : private boost::noncopyable
{
public:

    // Start a client with list of known brokers as hostname:port[,hostname:port[,...]]
    // If you miss out port for anyone we assume default 9092
    // Most uses probaly only need one background io thread for networking but increasing it
    // *might* improve latency/throughput in some situations
    ProducerClient(const std::string& brokers, int num_io_threads = 1);
    ~ProducerClient();

    // Set the timeout in milliseconds sent to kafka to wait for acks.
    // Default (below) is 10 seconds
    void set_produce_timeout(int32_t milliseconds);

    // Set timeout in milliseconds used when trying to connect to a broker.
    // This is used to limit the time waiting for both TCP connection and a response for metadata separatly.
    // i.e. it should be long enough for metadata transfer rather than just pure TCP handshake.
    // It also implies it might in pathalogical case take 2 * this long before we timeout if we connect sucessfully in
    // just less than the timeout, and then timeout actually fetching metadata.
    // Default is 1 second
    // Note that we may wait this long for EACH broker configured in
    // the string passed to constructor before a call that needs new meta will unblock.
    // For example check_topic_partition_leader_available() will need to re-load meta and if
    // all brokers are down we might wait up to this long for every configured broker to timeout
    // when we attempt to reconnect. In normal operation, we will simply re-use any already open
    // connections to reload meta which should be very fast.
    void set_connect_timeout(int32_t milliseconds);

    // Set how many attempts to retry we should make if we encounter an error when connecting or fetching metadata.
    // 0 means no retries.
    // There is no delay between retries since it is intended to function in such a way that it will retry with
    // a different broker. Periodic retry is left to the caller to implement.
    // It is not gauranteed that each retry attempt will be to a different broker (for example if only one broker is known)
    // Default is 1 retry (i.e. total of 2 attempts to connect/fetch meta)
    void set_retry_attempts(int32_t attempts);

    // Set timeout in milliseconts used to allow for network RTT on top of
    // produce_timeout. This is necessary because Kafka will wait for minimum
    // of produce_timeout for required acks before responding so we need to allow
    // additional time to allow for network traffic or slight delays in kafka due to overload
    // e.t.c. default is 500ms
    void set_produce_timeout_rtt_allowance(int32_t milliseconds);

    // How many replicas must ack each produce request. See Kafka docs for more info.
    // Default is -1 (wait for all in sync replicas to ack)
    void set_required_acks(int16_t acks);

    // Sent to kafka with each request see
    // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Requests
    // Defaults to "synkafka_client"
    void set_client_id(std::string client_id);

private:
    // Defaults
    int32_t produce_timeout_                = 10000;
    int32_t produce_timeout_rtt_allowance_  = 500;
    int32_t connect_timeout_                = 1000;
    int16_t required_acks_                  = -1;
    int32_t retry_attempts_                 = 1;

public:

    // Check if we have a known leader and are able to connect to it
    // for a given topic partition.
    // Note that this may attempt to connect to broker if we have no connection and so may
    // block for up to the connect_timeout.
    // Non-error return value of 0 means we are all good, otherwise assume not available and inspect category and code for reason.
    // category may be synkafka_error, kafka_error or system... std::errc conditions are defined for cases like timeout and
    // invalid config.
    std::error_code check_topic_partition_leader_available(const std::string& topic, int32_t partition_id);

    // Overload allows caller to obtain actual nodeid of the leader if it is available.
    // Use with care, this is really only exposed to allow for thorough testing.
    std::error_code check_topic_partition_leader_available(const std::string& topic, int32_t partition_id, int32_t* leader_id);

    // Synchronously produce a batch of messages
    // We assume the messages were already bult using the MessageSet class which validates
    // for known issues like maximum message size.
    // The caller must be configured to set max_message_size on the MessageSet correctly...
    // The returned error_code
    std::error_code produce(const std::string& topic, int32_t partition_id, MessageSet& messages);

    // Stop client and it's worker threads. Disconnects. The object cannot be used again after this is called.
    void close();

    // Parse broker structs form config string. Used in constructor, public mostly for testing
    static std::deque<proto::Broker> string_to_brokers(const std::string& brokers);

    // The internal asio thread entry point, should not be used outside of class
    // although must be public for std::thread to run.
    void run_asio();
private:

    struct Partition
    {
        std::string    topic;
        int32_t partition_id;

        bool operator==(const Partition& other) const
        {
            return (topic == other.topic
                    && partition_id == other.partition_id);
        }

        bool operator<(const Partition& other) const
        {
            if (topic == other.topic) {
                return partition_id < other.partition_id;
            }
            return topic < other.topic;
        }
    };

    struct BrokerContainer
    {
        proto::Broker                 config;
        std::shared_ptr<Broker>     broker;
    };

    std::shared_ptr<Broker> get_broker_for_partition(const Partition& p, bool refresh_meta = true);
    void close_broker(std::shared_ptr<Broker> broker);
    void refresh_meta(int attempts = 0);

    // Caller MUST hold lock on mu_
    std::string debug_dump_meta();

    std::deque<proto::Broker>                           broker_configs_; // Only the brokers that were initially passed as bootstrap - we don't have ids just host/ports
    std::map<int32_t, BrokerContainer>                  brokers_;
    std::map<Partition, int32_t>                        partition_map_;
    std::mutex                                          mu_; // protects all internal state - any state reads should be synchronized

    // If multiple threads waiting on meta data ensure only one connects
    // and others wait for it
    std::mutex                                          meta_fetch_mu_;
    std::chrono::time_point<std::chrono::system_clock>  last_meta_fetch_;
    std::error_code                                     last_meta_error_;

    boost::asio::io_service                             io_service_;
    std::unique_ptr<boost::asio::io_service::work>      work_;
    std::vector<std::thread>                            asio_threads_;
    std::atomic<bool>                                   stopping_;

    std::string                                         client_id_;
};

}