
#include <algorithm>
#include <cstring>
#include <exception>
#include <random>
#include <iomanip>
#include <set>
#include <system_error>
#include <sstream>

#include "log.h"
#include "synkafka.h"

namespace synkafka {

ProducerClient::ProducerClient(const std::string& brokers, int num_io_threads)
    :broker_configs_()
    ,brokers_()
    ,partition_map_()
    ,mu_()
    ,meta_fetch_mu_()
    ,last_meta_fetch_()
    ,last_meta_error_()
    ,produce_timeout_(10000)
    ,produce_timeout_rtt_allowance_(500)
    ,connect_timeout_(1000)
    ,required_acks_(-1)
    ,retry_attempts_(1)
    ,io_service_()
    ,work_(new boost::asio::io_service::work(io_service_))
    ,asio_threads_(num_io_threads)
    ,stopping_(false)
    ,client_id_("synkafka_client")
{
    broker_configs_ = string_to_brokers(brokers);

    // Randomize broker order so we "load balance" meta requests
    // among them if multiple are given.
    std::random_device rd;
    std::mt19937 g(rd());

    std::shuffle(broker_configs_.begin(), broker_configs_.end(), g);

    for (auto& t : asio_threads_) {
        t = std::thread(&ProducerClient::run_asio, this);
    }
}

ProducerClient::~ProducerClient()
{
    // If we are not stopped already then stop
    // note that if any thread is still using this object
    // when it is destructed then you have bigger problems
    // this just ensures threads are cleaned up.
    close();
}

void ProducerClient::set_produce_timeout(int32_t milliseconds)
{
    produce_timeout_ = milliseconds;
}

void ProducerClient::set_connect_timeout(int32_t milliseconds)
{
    connect_timeout_ = milliseconds;
}

void ProducerClient::set_retry_attempts(int32_t attempts)
{
    retry_attempts_ = attempts;
}

void ProducerClient::set_produce_timeout_rtt_allowance(int32_t milliseconds)
{
    produce_timeout_rtt_allowance_ = milliseconds;
}

void ProducerClient::set_required_acks(int16_t acks)
{
    required_acks_ = acks;
}

void ProducerClient::set_client_id(std::string client_id)
{
    client_id_ = std::move(client_id);
}

std::error_code ProducerClient::check_topic_partition_leader_available(const std::string& topic, int32_t partition_id)
{
    return check_topic_partition_leader_available(topic, partition_id, nullptr);
}

std::error_code ProducerClient::check_topic_partition_leader_available(const std::string& topic, int32_t partition_id, int32_t* leader_id)
{
    if (stopping_.load()) {
        return make_error_code(synkafka_error::client_stopping);
    }

    Partition p{topic, partition_id};

    auto broker = get_broker_for_partition(p);

    if (broker == nullptr) {
        if (last_meta_error_) {
            return last_meta_error_;
        }
        return make_error_code(kafka_error::UnknownTopicOrPartition);
    }

    // We got a broker! Try to connect (returns immediately if already connected)
    broker->set_connect_timeout(connect_timeout_);
    auto ec = broker->connect();

    if (ec) {
        close_broker(std::move(broker));
    } else {
        if (leader_id != nullptr) {
            *leader_id = broker->get_config().node_id;
        }
    }

    return ec;
}

std::error_code ProducerClient::produce(const std::string& topic, int32_t partition_id, MessageSet& messages)
{
    if (stopping_.load()) {
        return make_error_code(synkafka_error::client_stopping);
    }

    Partition p{topic, partition_id};

    auto broker = get_broker_for_partition(p);

    if (broker == nullptr) {
        if (last_meta_error_) {
            return last_meta_error_;
        }
        return make_error_code(kafka_error::UnknownTopicOrPartition);
    }

    // We got a broker! Try to connect (returns immediately if already connected)
    broker->set_connect_timeout(connect_timeout_);
    auto ec = broker->connect();

    if (ec) {
        close_broker(std::move(broker));
        return ec;
    }

    // We have a connected broker that is (at least last time we checked) leader
    // for the partition. Send it batch!
    proto::ProduceRequest rq{required_acks_
                            ,produce_timeout_
                            ,{proto::ProduceTopic{topic
                                                 ,{proto::ProducePartition{partition_id
                                                                           ,messages
                                                                           }
                                                  }
                                                 }
                             }
                            };


    proto::ProduceResponse resp;

    ec = broker->sync_call(rq, resp, produce_timeout_ + produce_timeout_rtt_allowance_);

    if (ec) {
        // All call error cases are client or network failures. Wipe out connection and hope
        // we can do better next time.
        close_broker(std::move(broker));
        return ec;
    }

    // OK got a response, see if it is kafka-protocol error!
    // We only ever produce one partition/topic at a time which makes this simpler.
    // In fact that is the whole point of this library - no partial failures
    if (!ec) {
        if (resp.topics.size() != 1 || resp.topics[0].partitions.size() != 1) {
            // Probably not possible?
            ec = make_error_code(synkafka_error::unknown);
        } else {
            ec = resp.topics[0].partitions[0].err_code;
        }
    }

       if (ec) {
           // All Kafka errors here are either transient or related to incorrect metadata
           // or bad messages. There is really no need to close broker.

           if (ec == kafka_error::NotLeaderForPartition) {
               // Reload metadata, we were clearly out of date.
               // TODO: For now still return the error to client - they can however retry
               // and it *should* work since we reloaded. If we retied ourselves, we'd
               // need to keep track of how many times just in case we got stuck in some
               // leader election loop of hell, we'd also need to verify that refreshing meta
               // actually worked otherwise we could be stuck in loop. For now just let client
               // figure that out depending on what makes sense for them. We might need to expose
               // meta refresh failures in that case?
               log()->warn("Metadata is stale broker ") << broker->get_config().node_id
                   << " is not leader for [" << topic << "," << partition_id << "]";
               refresh_meta();
           }
           return ec;
       }

    return make_error_code(synkafka_error::no_error);
}

std::shared_ptr<Broker> ProducerClient::get_broker_for_partition(const Partition& p, bool refresh_meta)
{
    std::unique_lock<std::mutex> lk(mu_);

    auto partition_it = partition_map_.find(p);
    if (partition_it == partition_map_.end() || partition_it->second < 0) {
        log()->debug("Don't know about partition (or no leader was elected yet) [") << p.topic << "," << p.partition_id << "] refresh meta: " << refresh_meta
            << "\n" << debug_dump_meta();
        // Don't know about that partition, re-fetch metadata?
        if (refresh_meta) {
            // Unlock so that refresh and recursive call can get lock
            lk.unlock();
            this->refresh_meta();
            // try again, but don't trigger another refetch if we failed
            return get_broker_for_partition(p, false);
        }
        // Don't know it, return a null ptr to broker
        return std::shared_ptr<Broker>(nullptr);
    }

    // We know that partition, lets find it's broker
    auto broker_it = brokers_.find(partition_it->second);
    if (broker_it == brokers_.end()) {
        // Brokers list doesn't have entry, this can't happen (tm)
        // since meta data fetch should always update both and Kafka should never
        // return a partition assigned to a broker that it doesn't also have in the cluster
        lk.unlock();
        close();
        throw std::runtime_error("No broker object made for a known partition. Kafka is trolling you or there is a bug. Closing client.");
    }

    if (broker_it->second.broker == nullptr
        || broker_it->second.broker->is_closed()) {
        // We have a null broker pointer which means it's not connected yet, create a new instance...
        // Or it already failed and got disconnected internally, so we reset.
        broker_it->second.broker = std::make_shared<Broker>(io_service_
                                                           ,broker_it->second.config.host
                                                           ,broker_it->second.config.port
                                                           ,client_id_
                                                           );
        broker_it->second.broker->set_node_id(broker_it->first);
    }

    return broker_it->second.broker;
}

void ProducerClient::close_broker(std::shared_ptr<Broker> broker)
{
    // Failed connection or timeout
    // We should clean up the broker (close it as it's now failed, and reset our pointer so it is removed)
    std::lock_guard<std::mutex> lk(mu_);

    // Close broker
    broker->close();

    // Must go and locate this broker in the map if it's there and reset it
    auto broker_it = brokers_.find(broker->get_config().node_id);
    if (broker_it != brokers_.end()) {
            if (broker_it->second.broker == broker) {
                // Same broker pointer still in map, reset it to free the broker instance
                // We will auto-create a new instance when someone next tries to connect to it
                broker_it->second.broker.reset();
            }
    }

    // Mark metadata as dirty so next call, reloads it
    // we don't do it here in case this was being closed after a timeout and we will exceed time
    // blocked by also waiting to refresh meta.
    // This is necessary in several cases including if the leader for a partition dies: in this case
    // we must reload meta to discover who new leader is. Without this we would be stuck trying to contact
    // old master indefinitely.
    partition_map_.clear();
}

void ProducerClient::refresh_meta(int attempts)
{
    // Get current time that we requested new meta (BEFORE lock)
    auto requested_at = std::chrono::system_clock::now();

    // Now acquire mutex to ensure we are the only thread fetching meta
    // (if we are not only thread then we will block here until other thread returns)
    std::unique_lock<std::mutex> meta_lock(meta_fetch_mu_);

    // We got the lock, double check if someone else completed refresh since we started acquire
    if (last_meta_fetch_ >= requested_at) {
        // Some other thread completed a meta fetch while we were waiting on lock. No need to
        // do it ourselves.
        log()->debug("ProducerClient thread was waiting on meta update that happened elsewhere");
        return;
    }

    // Fetch meta data from one connected broker. If there are none, bootstrap from the initial config list
    std::shared_ptr<Broker> broker;

    {
        std::lock_guard<std::mutex> lk(mu_);

        // First non-null broker will do...
        for (auto& b : brokers_) {
            if (b.second.broker != nullptr && !b.second.broker->is_closed()) {
                broker = b.second.broker;
                break;
            }
        }

        if (broker == nullptr) {
            // No connected brokers, try each one from config (they are randomly ordered on construction)
            for (auto& cfg : broker_configs_) {
                // Try creating a broker from this config and see if we can fetch metadata in the timeout
                broker = std::make_shared<Broker>(io_service_
                                                 ,cfg.host
                                                 ,cfg.port
                                                 ,client_id_
                                                 );

                broker->set_connect_timeout(connect_timeout_);
                last_meta_error_ = broker->connect();

                if (!last_meta_error_) {
                    // Connected OK. we are done.
                    break;
                }
            }
        }

        if (broker == nullptr || broker->is_closed()) {
            // Still not connected? not much more we can do
            return;
        }
    }

    // Now actually fetch some meta-data from the broker we got a connection to.
    // For now we always fetch all topics for simplicity...
    proto::TopicMetadataRequest req;
    proto::MetadataResponse resp;

    // Re-use connect timeout for meta data since meta fetch is really only an implementation specific step in getting connected to
    // correct node. This is documented in the public API in header file.
    std::error_code ec = broker->sync_call(req, resp, connect_timeout_);

    if (ec) {
        // Close and reset broker that failed
        close_broker(std::move(broker));

        {
            std::lock_guard<std::mutex> lk(mu_);
            last_meta_error_ = ec;
        }

        if (attempts < retry_attempts_) {
            meta_lock.unlock();
            return refresh_meta(attempts + 1);
        }
        return;
    }

    // Now build our internal data structures

    // First lets add Broker objects if we don't know about them already
    {
        std::lock_guard<std::mutex> lk(mu_);

        // No error, clear last error
        last_meta_error_ = make_error_code(synkafka_error::no_error);

        std::set<int32_t> live_broker_ids;

        for (auto& broker : resp.brokers) {
            live_broker_ids.insert(broker.node_id);

            auto broker_it = brokers_.find(broker.node_id);
            if (broker_it == brokers_.end()) {
                // New broker, add it
                brokers_.insert(std::make_pair(broker.node_id
                                              ,BrokerContainer{broker
                                                                ,{nullptr}
                                                                }
                                              )
                               );
            } else {
                // We already know of broker by that id, sanity check it's still configured the same...
                if (broker_it->second.config.host != broker.host || broker_it->second.config.port != broker.port) {
                    // disconnect and create a new one...
                    broker_it->second.broker->close();
                    broker_it->second.broker.reset();
                }
            }
        }

        if (live_broker_ids.size() < brokers_.size()) {
            // Some brokers have been removed from cluster remove them from our state too
            for (auto b_it = brokers_.cbegin(); b_it != brokers_.end(); /* no increment */) {
                if (live_broker_ids.count(b_it->first) == 0) {
                    if (b_it->second.broker) {
                        b_it->second.broker->close();
                    }
                    brokers_.erase(b_it++);
                } else {
                    ++b_it;
                }
            }
        }

        // Now update partition map too
        std::map<Partition, int32_t> new_map;

        for (auto& topic : resp.topics) {
            for (auto& part : topic.partitions) {
                new_map.insert(std::make_pair(Partition{topic.name, part.partition_id}, part.leader));
            }
        }

        // Swap!
        partition_map_.swap(new_map);

        log()->info("Updated Cluster Meta:\n") << debug_dump_meta();
    }
    last_meta_fetch_ = std::chrono::system_clock::now();
}

void ProducerClient::close()
{
    {
        std::lock_guard<std::mutex> lg(mu_);
        stopping_ = true;
    }

    // Clear io service work to allow it to stop
    work_.reset();

    // Stop io service
    io_service_.stop();

    // Wait for all asio thread to stop
    // Currently this is a somewhat graceful shutdown - we stop making new requests but we wait
    // for all brokers to finish any outstanding reads/writes they are making.
    // TODO: figure out if that works as expected and if there is any case where we might wait
    // longer than the produce_timeout to stop?
    for (auto& t : asio_threads_) {
        t.join();
    }
}

void ProducerClient::run_asio()
{
    try
    {
        io_service_.run();
        log()->info("synkafka::ProducerClient shutting asio thread shutdown cleanly");
    }
    catch (const std::error_code& e)
    {
        log()->error("ProducerClient asio thread exits with error_code ") << e.value() << ": " << e.message();
    }
    catch (const std::exception& e)
    {
        log()->error("ProducerClient asio thread exits with exception: ") << e.what();
    }
    catch (...)
    {
        log()->error("ProducerClient asio thread exits with unknown exception");
    }
}

// Caller MUST hold lock on mu_
std::string ProducerClient::debug_dump_meta()
{
    std::stringstream dump("Brokers:");

    for (auto& pair : brokers_) {
        dump << std::setw(5) << std::setfill(' ') << pair.first
             << "\t" << pair.second.config.host << ":" << pair.second.config.port
             << " connected: " << (pair.second.broker && pair.second.broker->is_connected() ? 'y' : 'n')
             << " closed: " << (pair.second.broker && pair.second.broker->is_closed() ? 'y' : 'n')
             << std::endl;
    }

    for (auto& pair : partition_map_) {
        dump << std::setw(10) << std::setfill(' ') << pair.first.topic
             << "(" << pair.first.partition_id << ") -> "
             << std::setw(5) << std::setfill(' ') << pair.second
             << std::endl;
    }

    return dump.str();
}

std::deque<proto::Broker> ProducerClient::string_to_brokers(const std::string& brokers)
{
    std::deque<proto::Broker> brokers_out;
    std::istringstream brokers_stream(brokers);
    std::string broker;

    while(std::getline(brokers_stream, broker, ',')) {
        std::istringstream broker_stream(broker);
        std::string host, port;
        std::getline(broker_stream, host, ':');
        std::getline(broker_stream, port);
        if(port.empty()) {
            port = "9092";
        }
        brokers_out.push_back({0, host, std::stoi(port)});
    }

    return brokers_out;
}

}