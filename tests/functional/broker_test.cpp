#include "gtest/gtest.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <thread>

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>

#include "broker.h"
#include "connection.h"
#include "packet.h"
#include "protocol.h"

#include "test_cluster.h"

using namespace synkafka;

class BrokerTest : public ::testing::Test
{
public:
    void run_asio()
    {
        try
        {
            io_service_.run();
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

protected:
    virtual void SetUp()
    {
        work_.reset(new boost::asio::io_service::work(io_service_));
        asio_thread_ = std::thread(&BrokerTest::run_asio, this);
    }

    virtual void TearDown()
    {    
        work_.reset(); // Exit asio thread
        io_service_.stop();
        asio_thread_.join();
    }

    boost::asio::io_service                         io_service_;
    std::auto_ptr<boost::asio::io_service::work>    work_;
    std::thread                                     asio_thread_;
};



TEST_F(BrokerTest, ConnectionWorks) {
    Connection conn(io_service_, get_env_string("KAFKA_1_HOST"), get_env_int("KAFKA_1_PORT"));
    conn.set_timeout(100);

    auto err = conn.connect();
    EXPECT_FALSE(err);
}

TEST_F(BrokerTest, ConnectionTimesOut)
{
    Connection conn(io_service_, "192.0.2.0", get_env_int("KAFKA_1_PORT"));
    conn.set_timeout(100);

    auto err = conn.connect();
    EXPECT_EQ(boost::system::errc::timed_out, err);
}

TEST_F(BrokerTest, ConnectionTimesOutMultiThreaded)
{
    std::vector<std::thread> threads(5);

    Connection conn(io_service_, "192.0.2.0", get_env_int("KAFKA_1_PORT"));
    conn.set_timeout(100);

    for (int i = 0; i < 5 ; ++i) {
        threads[i] = std::thread([&](){
            auto err = conn.connect();
            EXPECT_EQ(boost::system::errc::timed_out, err);
        });
    }

    for (int i = 0; i < 5 ; ++i) {
        threads[i].join();
    }
}

TEST_F(BrokerTest, ConnectionWorksMultiThreaded)
{
    std::vector<std::thread> threads(5);

    Connection conn(io_service_, get_env_string("KAFKA_1_HOST"), get_env_int("KAFKA_1_PORT"));
    conn.set_timeout(100);

    for (int i = 0; i < 5 ; ++i) {
        threads[i] = std::thread([&](){
            auto err = conn.connect();
            EXPECT_FALSE(err);
        });
    }

    for (int i = 0; i < 5 ; ++i) {
        threads[i].join();
    }
}


TEST_F(BrokerTest, MetadataRequest)
{
    boost::shared_ptr<Broker> b(new Broker(io_service_, get_env_string("KAFKA_1_HOST"), get_env_int("KAFKA_1_PORT"), "test"));

    auto err = b->connect();
    ASSERT_FALSE(err);

    proto::TopicMetadataRequest rq;
    proto::MetadataResponse resp;
    auto enc = std::make_shared<PacketEncoder>(128);
    enc->io(rq);

    auto decoder_future = b->call(ApiKey::MetadataRequest, std::move(enc));

    auto status = decoder_future.wait_for(std::chrono::seconds(1));

    ASSERT_NE(std::future_status::timeout, status);

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

void test_connect_and_send_message_set(boost::asio::io_service& io_service
									  ,proto::Broker& broker
									  ,MessageSet& ms
									  ,int32_t partition_id
									  ,const char* test_name
									  ,proto::ProduceResponse* resp
									  )
{
	boost::shared_ptr<Broker> b(new Broker(io_service, broker.host, broker.port, "test"));

    auto err = b->connect();
    ASSERT_FALSE(err);

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

    ASSERT_EQ(std::future_status::ready, status);

    auto decoder = decoder_future.get();
    decoder.io(*resp);

    EXPECT_TRUE(decoder.ok());		    
    EXPECT_EQ(1, 					resp->topics.size());
	EXPECT_EQ("test", 				resp->topics[0].name);
	EXPECT_EQ(1, 					resp->topics[0].partitions.size());
	EXPECT_EQ(0, 					resp->topics[0].partitions[0].partition_id);
}


TEST_F(BrokerTest, SimpleProduce)
{
    // First we need to get meta data to find out which broker to produce to
    boost::shared_ptr<Broker> b(new Broker(io_service_, get_env_string("KAFKA_1_HOST"), get_env_int("KAFKA_1_PORT"), "test"));
    //boost::shared_ptr<Broker> b(new Broker(io_service, "localhost", 9000, "test"));

    auto err = b->connect();
    ASSERT_FALSE(err);

    proto::TopicMetadataRequest rq;
    proto::MetadataResponse resp;
    auto enc = std::make_shared<PacketEncoder>(128);
    enc->io(rq);

    auto decoder_future = b->call(ApiKey::MetadataRequest, std::move(enc));

    auto status = decoder_future.wait_for(std::chrono::seconds(1));

    ASSERT_EQ(std::future_status::ready, status);

	// Find broker for partition 0 and produce to it
	// (we rely on there being correct partitions as tested in meta test etc)
	auto decoder = decoder_future.get();
    decoder.io(resp);

    EXPECT_TRUE(decoder.ok());

	proto::Broker* test_0_leader = nullptr;
    proto::Broker* test_0_not_leader = nullptr;
	proto::Broker* test_0_not_replica = nullptr;

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
						} else {
                            bool is_replica = (std::find(partition.replicas.begin()
                                                        ,partition.replicas.end()
                                                        ,broker.node_id
                                                        ) != partition.replicas.end());

                            if (test_0_not_leader == nullptr && is_replica) {
                                test_0_not_leader = &broker;
                            }
                            if (test_0_not_replica == nullptr && !is_replica) {
                                test_0_not_replica = &broker;
                            }
                        }
					}
					break;
				}
			}
		break;
		}
	}

	ASSERT_NE(nullptr, test_0_leader);
    ASSERT_NE(nullptr, test_0_not_leader);
	ASSERT_NE(nullptr, test_0_not_replica);

	MessageSet ms;

	ms.push("Hello World. This is a Message produced by Synkafka. 1.", "");
	ms.push("Hello World. This is a Message produced by Synkafka. 2.", "");
	ms.push("Hello World. This is a Message produced by Synkafka. 3.", "");
	ms.push("Hello World. This is a Message produced by Synkafka. 4.", "");

    {
    	proto::ProduceResponse resp;
    	test_connect_and_send_message_set(io_service_, *test_0_leader, ms, 0, "uncompressed", &resp);
        EXPECT_EQ(kafka_error::NoError, resp.topics[0].partitions[0].err_code);
    }

    // Test publish to non-leader gets expected error
    {
        proto::ProduceResponse resp;
        test_connect_and_send_message_set(io_service_, *test_0_not_leader, ms, 0, "uncompressed, non-leader broker", &resp);
        EXPECT_EQ(kafka_error::NotLeaderForPartition, resp.topics[0].partitions[0].err_code);
    }

	// Test publish to non-replica gets expected error
	{
        proto::ProduceResponse resp;
    	test_connect_and_send_message_set(io_service_, *test_0_not_replica, ms, 0, "uncompressed, non-replica broker", &resp);
        EXPECT_EQ(kafka_error::UnknownTopicOrPartition, resp.topics[0].partitions[0].err_code);
    }

	// Test publish with GZIP
    {
    	ms.push("Hello World. This is extra message for GZIP batch", "");
    	ms.set_compression(COMP_GZIP);
    	proto::ProduceResponse resp;
    	test_connect_and_send_message_set(io_service_, *test_0_leader, ms, 0, "GZIP", &resp);
        EXPECT_EQ(kafka_error::NoError, resp.topics[0].partitions[0].err_code);
    }

	//And Snappy
    {
    	ms.push("Hello World. This is extra message for Snappy batch", "");
    	ms.set_compression(COMP_Snappy);
    	proto::ProduceResponse resp;
    	test_connect_and_send_message_set(io_service_, *test_0_leader, ms, 0, "Snappy", &resp);
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

