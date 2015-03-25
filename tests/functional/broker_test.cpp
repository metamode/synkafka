#include "gtest/gtest.h"

#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <thread>

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>

#include "broker.h"
#include "packet.h"
#include "protocol.h"

#include "test_cluster.h"

using namespace synkafka;

void run_asio(boost::asio::io_service& io_service)
{
	try
	{
    	io_service.run();
		//std::cout << "ASIO THREAD EXITING NORMALLY" << std::endl;
	}
	catch (const std::exception& e)
	{
		std::cout << "ASIO THREAD EXCEPTION: " << e.what() << std::endl;
	}
	catch (...)
	{
		std::cout << "ASIO THREAD UNKNOWN EXCEPTION" << std::endl;
	}
}

TEST(Broker, MetadataRequest)
{
    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(
    	new boost::asio::io_service::work(io_service)); // Keep io service running till we are done

    std::thread ios_thread(run_asio, std::ref(io_service));

    boost::shared_ptr<Broker> b(new Broker(io_service, get_env_string("KAFKA_1_HOST"), get_env_int("KAFKA_1_PORT"), "test"));

    b->start_connect();

    proto::TopicMetadataRequest rq;
    proto::MetadataResponse resp;
    auto enc = std::make_shared<PacketEncoder>(128);
    enc->io(rq);

    auto decoder_future = b->call(ApiKey::MetadataRequest, std::move(enc));

    auto status = decoder_future.wait_for(std::chrono::seconds(1));

    EXPECT_NE(std::future_status::timeout, status);

    if (std::future_status::timeout != status) {
	    auto decoder = decoder_future.get();
	    decoder.io(resp);

	    EXPECT_TRUE(decoder.ok());

	    auto broker_num = get_env_int("KAFKA_BROKER_NUM");

	    EXPECT_EQ(broker_num, resp.brokers.size());

	    std::vector<std::string> broker_list;
	    for (auto& broker : resp.brokers) {
	    	// Rebuild the broker list from the fetched meta so we can normalise ordering
	    	// which is undefined, rely on our test config always assigning same IP and port in ascending order
	    	broker_list.push_back(broker.host + ":" + std::to_string(broker.port) + ":" + std::to_string(broker.node_id));
	    }

	    std::sort(broker_list.begin(), broker_list.end());

    	// Rely on our test setup assigning node id based on port
    	EXPECT_EQ(get_env_string("KAFKA_1_HOST")
    				+ ":" + std::to_string(get_env_int("KAFKA_1_PORT")) 
    				+ ":" + std::to_string(get_env_int("KAFKA_1_PORT"))
    			 ,broker_list[0]
    			 );
       	EXPECT_EQ(get_env_string("KAFKA_2_HOST")
    				+ ":" + std::to_string(get_env_int("KAFKA_2_PORT")) 
    				+ ":" + std::to_string(get_env_int("KAFKA_2_PORT"))
    			 ,broker_list[1]
    			 );
       	EXPECT_EQ(get_env_string("KAFKA_3_HOST")
    				+ ":" + std::to_string(get_env_int("KAFKA_3_PORT")) 
    				+ ":" + std::to_string(get_env_int("KAFKA_3_PORT"))
    			 ,broker_list[2]
    			 );

	    // Expect one topic called test with 8 partitions...
	    EXPECT_EQ(1, resp.topics.size());
	    EXPECT_EQ("test", resp.topics[0].name);
	    EXPECT_EQ(8, resp.topics[0].partitions.size());

	    // Partitions should have 2 replicas
	    EXPECT_EQ(2, resp.topics[0].partitions[0].replicas.size());

	    // Partitions should have 2 in sync replicas
	    EXPECT_EQ(2, resp.topics[0].partitions[0].isr.size());
    }

    work.reset(); // Exit asio thread
    io_service.stop();
    ios_thread.join();
}

bool test_connect_and_send_message_set(boost::asio::io_service& io_service
									  ,proto::Broker& broker
									  ,MessageSet& ms
									  ,int32_t partition_id
									  ,const char* test_name
									  ,proto::ProduceResponse* resp
									  )
{
	boost::shared_ptr<Broker> b(new Broker(io_service, broker.host, broker.port, "test"));

	b->start_connect();

    proto::ProduceRequest rq{1
    						,500
    						,{proto::ProduceTopic{"test"
    											 ,{proto::ProducePartition{partition_id
    											 						  ,ms
    											 						  }
    											  }
    											 }
    						 }
    						};

    auto enc = std::make_shared<PacketEncoder>(1024);
    enc->io(rq);

    EXPECT_TRUE(enc->ok()) << test_name << "Encoder error: " << enc->err_str();

    auto decoder_future = b->call(ApiKey::ProduceRequest, std::move(enc));

    auto status = decoder_future.wait_for(std::chrono::seconds(1));

    EXPECT_NE(std::future_status::timeout, status);

    if (std::future_status::timeout != status) {
	    auto decoder = decoder_future.get();
	    decoder.io(*resp);

	    EXPECT_TRUE(decoder.ok());		    
	    EXPECT_EQ(1, 					resp->topics.size());
		EXPECT_EQ("test", 				resp->topics[0].name);
		EXPECT_EQ(1, 					resp->topics[0].partitions.size());
		EXPECT_EQ(0, 					resp->topics[0].partitions[0].partition_id);

	    return decoder.ok();
    }
    return false;
}


TEST(Broker, SimpleProduce)
{
    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(
    	new boost::asio::io_service::work(io_service)); // Keep io service running till we are done

    std::thread ios_thread(run_asio, std::ref(io_service));


    // First we need to get meta data to find out which broker to produce to
    boost::shared_ptr<Broker> b(new Broker(io_service, get_env_string("KAFKA_1_HOST"), get_env_int("KAFKA_1_PORT"), "test"));
    //boost::shared_ptr<Broker> b(new Broker(io_service, "localhost", 9000, "test"));

    b->start_connect();

    proto::TopicMetadataRequest rq;
    proto::MetadataResponse resp;
    auto enc = std::make_shared<PacketEncoder>(128);
    enc->io(rq);

    auto decoder_future = b->call(ApiKey::MetadataRequest, std::move(enc));

    auto status = decoder_future.wait_for(std::chrono::seconds(1));

    EXPECT_NE(std::future_status::timeout, status);

    if (std::future_status::timeout != status) {

    	// Find broker for partition 0 and produce to it
    	// (we rely on there being correct partitions as tested in meta test etc)
    	auto decoder = decoder_future.get();
	    decoder.io(resp);

	    EXPECT_TRUE(decoder.ok());

    	proto::Broker* test_0_leader = nullptr;
    	proto::Broker* test_0_not_leader = nullptr;

    	EXPECT_LT(0, resp.brokers.size());
    	EXPECT_LT(0, resp.topics.size());

    	for (auto& topic : resp.topics) {
    		if (topic.name == "test") {
    			for (auto& partition : topic.partitions) {
    				if (partition.partition_id == 0) {
    					// Get leader
    					for (auto& broker : resp.brokers) {
    						if (broker.node_id == partition.leader) {
    							test_0_leader = &broker;
    						} else if (test_0_not_leader == nullptr) {
    							test_0_not_leader = &broker;
    						}
    					}
    					break;
    				}
    			}
    		break;
    		}
    	}

    	EXPECT_NE(nullptr, test_0_leader);
    	EXPECT_NE(nullptr, test_0_not_leader);

		MessageSet ms;

		ms.push("Hello World. This is a Message produced by Synkafka. 1.", "");
		ms.push("Hello World. This is a Message produced by Synkafka. 2.", "");
		ms.push("Hello World. This is a Message produced by Synkafka. 3.", "");
		ms.push("Hello World. This is a Message produced by Synkafka. 4.", "");

    	if (test_0_leader) {    
    		proto::ProduceResponse resp;
    		EXPECT_TRUE(test_connect_and_send_message_set(io_service, *test_0_leader, ms, 0, "uncompressed", &resp));
		    EXPECT_EQ(kafka_error::NoError, resp.topics[0].partitions[0].err_code);
    	}

    	// Test publish to non-leader gets expected error
    	if (test_0_not_leader) {    
    		proto::ProduceResponse resp;
    		EXPECT_TRUE(test_connect_and_send_message_set(io_service, *test_0_not_leader, ms, 0, "uncompressed, wrong broker", &resp));
		    EXPECT_EQ(kafka_error::NotLeaderForPartition, resp.topics[0].partitions[0].err_code);
    	}

    	// Test publish with GZIP
		ms.push("Hello World. This is extra message for GZIP batch", "");
    	ms.set_compression(COMP_GZIP);
    	if (test_0_leader) {    
    		proto::ProduceResponse resp;
    		EXPECT_TRUE(test_connect_and_send_message_set(io_service, *test_0_leader, ms, 0, "GZIP", &resp));
		    EXPECT_EQ(kafka_error::NoError, resp.topics[0].partitions[0].err_code);
    	}  

    	//And Snappy
		ms.push("Hello World. This is extra message for Snappy batch", "");
    	ms.set_compression(COMP_Snappy);
    	if (test_0_leader) {    
    		proto::ProduceResponse resp;
    		EXPECT_TRUE(test_connect_and_send_message_set(io_service, *test_0_leader, ms, 0, "Snappy", &resp));
		    EXPECT_EQ(kafka_error::NoError, resp.topics[0].partitions[0].err_code);
    	}

    	// All of the above can be verified it's really doing what is expected manually via:
    	//  1. check the test run output when DEBUG logs are on and see that first two sends result in something like
    	//       handle_write sent OK (370 bytes written)
    	//     while GZIP and snappy send fewer bytes:
    	//       handle_write sent OK (227 bytes written)
    	//       handle_write sent OK (259 bytes written)
    	//  2. Verify using `./kafka-simple-consumer-shell.sh --broker-list=192.168.59.103:9092 --topic test --partition 0`
    	//     that the messages are readable and sent as expected. They also decode compressed ones transparently (hence differnt messages in those) 
    }

    work.reset(); // Exit asio thread
    io_service.stop();
    ios_thread.join();
}