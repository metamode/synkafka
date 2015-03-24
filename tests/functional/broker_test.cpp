#include "gtest/gtest.h"

#include <chrono>
#include <future>
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


TEST(Broker, SimpleProduce)
{
    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(
    	new boost::asio::io_service::work(io_service)); // Keep io service running till we are done

    std::thread ios_thread(run_asio, std::ref(io_service));

    boost::shared_ptr<Broker> b(new Broker(io_service, get_env_string("KAFKA_1_HOST"), get_env_int("KAFKA_1_PORT"), "test"));

    b->start_connect();

    MessageSet ms;

    ms.push("Hello World. This is a Message produced by Synkafka. 1.", "");
    ms.push("Hello World. This is a Message produced by Synkafka. 2.", "");
    ms.push("Hello World. This is a Message produced by Synkafka. 3.", "");
    ms.push("Hello World. This is a Message produced by Synkafka. 4.", "");

    proto::ProduceRequest rq{1
    						,1000
    						,{proto::ProduceTopic{"test"
    											 ,{proto::ProducePartition{0
    											 						  ,ms
    											 						  }
    											  }
    											 }
    						 }
    						};

    proto::ProduceResponse resp;
    auto enc = std::make_shared<PacketEncoder>(1024);
    enc->io(rq);

    EXPECT_TRUE(enc->ok()) << "Error: " << enc->err_str();

    auto decoder_future = b->call(ApiKey::MetadataRequest, std::move(enc));

    auto status = decoder_future.wait_for(std::chrono::seconds(1));

    EXPECT_NE(std::future_status::timeout, status);

    if (std::future_status::timeout != status) {
	    auto decoder = decoder_future.get();
	    decoder.io(resp);

	    EXPECT_TRUE(decoder.ok());

	    EXPECT_EQ(1, resp.topics.size());
	    EXPECT_EQ("test", resp.topics[0].name);
	    EXPECT_EQ(1, resp.topics[0].partitions.size());
	    EXPECT_EQ(0, resp.topics[0].partitions[0].partition_id);
	    EXPECT_EQ(kafka_error::NoError, resp.topics[0].partitions[0].err_code);
	    EXPECT_GT(0, resp.topics[0].partitions[0].offset);
    }

    work.reset(); // Exit asio thread
    io_service.stop();
    ios_thread.join();
}