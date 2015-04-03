
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

void ProducerClient::set_required_acks(int32_t acks)
{
	required_acks_ = acks;
}

void ProducerClient::set_client_id(const std::string& client_id)
{
	client_id_ = client_id;
}

std::error_code ProducerClient::check_topic_partition_leader_available(const slice& topic, int32_t partition_id)
{
	if (stopping_.load()) {
		return make_error_code(synkafka_error::client_stopping);
	}

	Partition p{topic, partition_id};

	auto broker = get_broker_for_partition(p);

	if (broker == nullptr) {
		return make_error_code(kafka_error::UnknownTopicOrPartition);
	}

	// We got a broker! Try to connect (returns immediately if allready connected)
	auto ec = broker->wait_for_connect(connect_timeout_);

	if (ec) {
		close_broker(std::move(broker));
	}

	return ec;
}

std::error_code ProducerClient::produce(const slice& topic, int32_t partition_id, MessageSet& messages)
{
	std::lock_guard<std::mutex> lg(mu_);

	if (stopping_) {
		return make_error_code(synkafka_error::client_stopping);
	}

	return make_error_code(synkafka_error::no_error);
}
	
boost::shared_ptr<Broker> ProducerClient::get_broker_for_partition(const Partition& p, bool refresh_meta)
{
	std::unique_lock<std::mutex> lk(mu_);

	auto partition_it = partition_map_.find(p);
	if (partition_it == partition_map_.end()) {
		// Don't know about that partition, re-fetch metadata?
		if (refresh_meta) {
			// Unlock so that refresh and recursive call can get lock
			lk.unlock();
			this->refresh_meta();
			// try again, but don't trigger another refetch if we failed
			return get_broker_for_partition(p, false);
		}
		// Don't know it, return a null ptr to broker
		return boost::shared_ptr<Broker>(nullptr);
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

	if (broker_it->second.broker == nullptr) {
		// We have a null broker pointer which means it's not connected yet, create a new instance...
		broker_it->second.broker.reset(new Broker(io_service_
												 ,broker_it->second.config.host
												 ,broker_it->second.config.port
												 ,client_id_
												 )
									  );
	}

	return broker_it->second.broker;
}

void ProducerClient::close_broker(boost::shared_ptr<Broker> broker)
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
}

void ProducerClient::refresh_meta(int attempts)
{
	// Fetch meta data from one connected broker. If there are none, bootstrap from the initial config list
	boost::shared_ptr<Broker> broker;

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
				// Try creating a broker from this config and see if we can fetch meta data in the timeout
				broker.reset(new Broker(io_service_
									   ,cfg.host
									   ,cfg.port
									   ,client_id_
									   ));

				auto ec = broker->wait_for_connect(connect_timeout_);

				if (!ec) {
					// Connected OK. we are done.
					break;
				}
			}
		}
	}

	// Now actually fetch some meta-data from the broker we got a connection to.
	// For now we always fetch all topics for simplicity...
	proto::TopicMetadataRequest req;
	proto::MetadataResponse resp;

	// Requests are tiny 32 bytes is enough for now
	auto enc = std::make_shared<PacketEncoder>(32);
	enc->io(req);

	if (!enc->ok()) {
		log->error("Failed to encode Metadata request: ") << enc->err_str();
		return;
	}

    auto decoder_future = broker->call(ApiKey::MetadataRequest, std::move(enc));

    // Re-use connect timeout for meta data since meta fetch is really only an implementation specific step in getting connected to
    // correct node. This is docuemented in the public API in header file.
    auto status = decoder_future.wait_for(std::chrono::milliseconds(connect_timeout_));

    bool failed = false;

    if (status != std::future_status::ready) {
    	failed = true;
		log->error("Failed to get Metadata: connection timedout, broker: ")
			<< broker->get_config().host << ":" << broker->get_config().port;
    } else {

	    // OK we got a result, decode it into our meta data
	    try 
	    {
	    	auto decoder = decoder_future.get();
		    decoder.io(resp);

			if (!decoder.ok()) {
				failed = true;
				log->error("Failed to get Metadata: decoder error: ") << decoder.err_str();
			}
	    }
	    catch (const std::error_code& ec)
	    {
	    	failed = true;
	    	log->error("Failed to get Metadata: ") << ec.message();
	    }
	    catch (const std::exception& e)
	    {
	    	failed = true;
	    	log->error("Failed to get Metadata: unexpected exception: ") << e.what();
	    }
	    catch (...)
	    {
	    	failed = true;
	    	log->error("Failed to get Metadata: unknown exception");
	    }
    }

    if (failed) {
    	// Close and reset broker that timed out
    	close_broker(std::move(broker));

		if (attempts < retry_attempts_) {
			return refresh_meta(attempts + 1);
		}
		log->error("Failed to get Metadata after ") << attempts + 1 << " attempts";
		return;
    }

	// Now build our internal data structures

	// First lets add Broker objects if we don't know about them already
	{
		std::lock_guard<std::mutex> lk(mu_);

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
					b_it->second.broker->close();
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
				new_map.emplace(Partition{topic.name, part.partition_id}, part.leader);
			}
		}

		// Swap!
		partition_map_.swap(new_map);
	}

	log->debug("Updated Cluster Meta:\n") << debug_dump_meta();
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
		log->info("synkafka::ProducerClient shutting asio thread shutdown cleanly");
	}
	catch (const std::error_code& e)
	{
		log->error("ProducerClient asio thread exits with error_code ") << e.value() << ": " << e.message();
	}
	catch (const std::exception& e)
	{
		log->error("ProducerClient asio thread exits with exception: ") << e.what();
	}
	catch (...)
	{
		log->error("ProducerClient asio thread exits with unknown exception");
	}
}

std::string ProducerClient::debug_dump_meta()
{
	std::lock_guard<std::mutex> lk(mu_);

	std::stringstream dump("Brokers:");

	for (auto& pair : brokers_) {
		dump << std::setw(5) << std::setfill(' ') << pair.first
			 << "\t" << pair.second.config.host << ":" << pair.second.config.port
			 << " connected: " << (pair.second.broker && pair.second.broker->is_connected() ? 'y' : 'n')
			 << " closed: " << (pair.second.broker && pair.second.broker->is_closed() ? 'y' : 'n')
			 << std::endl;
	}

	for (auto& pair : partition_map_) {
		dump << std::setw(10) << std::setfill(' ') << pair.first.topic.str()
			 << "(" << pair.first.partition_id << ") -> "
			 << std::setw(5) << std::setfill(' ') << pair.second
			 << std::endl;
	}

	return dump.str();
}

std::deque<proto::Broker> ProducerClient::string_to_brokers(const std::string& brokers)
{
	std::string host_string, port_string;
	std::deque<proto::Broker> brokers_out;

	bool in_host = true;

	for (auto& ch : brokers) {
		if (ch == ':') {
			in_host = false;
		} else if (ch == ',') {
			in_host = true;

			int32_t port = 9092;
			if (!port_string.empty()) {
				port = atoi(port_string.c_str());
			}

			brokers_out.push_back({0, host_string, port});

			host_string = "";
			port_string = "";
		} else {
			if (in_host) {
				host_string += ch;
			} else {
				port_string += ch;
			}
		}
	}

	int32_t port = 9092;
	if (!port_string.empty()) {
		port = atoi(port_string.c_str());
	}

	brokers_out.push_back({0, host_string, port});


	return brokers_out;
}

}