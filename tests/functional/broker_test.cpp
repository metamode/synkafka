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
#include "log.h"

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

    void SetUpProduce()
    {
        // First we need to get meta data to find out which broker to produce to
        boost::shared_ptr<Broker> b(new Broker(io_service_, get_env_string("KAFKA_1_HOST"), get_env_int("KAFKA_1_PORT"), "test"));

        auto err = b->connect();
        ASSERT_FALSE(err);

        proto::TopicMetadataRequest rq;

        err = b->sync_call(rq, meta_, 1000);
        ASSERT_FALSE(err)
            << "Request failed: " << err.message();

        test_0_leader_ = nullptr;
        test_0_not_leader_ = nullptr;
        test_0_not_replica_ = nullptr;

        EXPECT_LT(0ul, meta_.brokers.size());
        EXPECT_LT(0ul, meta_.topics.size());

        for (auto& topic : meta_.topics) {
            if (topic.name == "test") {
                for (auto& partition : topic.partitions) {
                    if (partition.partition_id == 0) {
                        // Get leader
                        for (auto& broker : meta_.brokers) {
                            if (broker.node_id == partition.leader) {
                                test_0_leader_ = &broker;
                            } else {
                                bool is_replica = (std::find(partition.replicas.begin()
                                                            ,partition.replicas.end()
                                                            ,broker.node_id
                                                            ) != partition.replicas.end());

                                if (test_0_not_leader_ == nullptr && is_replica) {
                                    test_0_not_leader_ = &broker;
                                }
                                if (test_0_not_replica_ == nullptr && !is_replica) {
                                    test_0_not_replica_ = &broker;
                                }
                            }
                        }
                        break;
                    }
                }
            break;
            }
        }

        ASSERT_NE(nullptr, test_0_leader_);
        ASSERT_NE(nullptr, test_0_not_leader_);
        ASSERT_NE(nullptr, test_0_not_replica_);

        ms_.reset(new MessageSet());
        ms_->push("Hello World. This is a Message produced by Synkafka. 1.", "");
        ms_->push("Hello World. This is a Message produced by Synkafka. 2.", "");
        ms_->push("Hello World. This is a Message produced by Synkafka. 3.", "");
        ms_->push("Hello World. This is a Message produced by Synkafka. 4.", "");
    }

    void test_connect_and_send_message_set(proto::Broker& broker
                                          ,int32_t partition_id
                                          ,std::error_code expected_err
                                          ,int num_batches_in_pipeline = 1
                                          )
    {
        boost::shared_ptr<Broker> b(new Broker(io_service_, broker.host, broker.port, "test"));

        auto err = b->connect();
        ASSERT_FALSE(err);

        std::vector<std::future<PacketDecoder>> futures;


        for (int i = 0; i < num_batches_in_pipeline; ++i) {
            proto::ProduceRequest rq{1
                                    ,500
                                    ,{proto::ProduceTopic{"test"
                                                         ,{proto::ProducePartition{partition_id
                                                                                  ,*ms_
                                                                                  }
                                                          }
                                                         }
                                     }
                                    };


            auto enc = std::unique_ptr<PacketEncoder>(new PacketEncoder(512));
            enc->io(rq);
            ASSERT_TRUE(enc->ok());

            futures.push_back(std::move(b->call(proto::ProduceRequest::api_key, std::move(enc))));
        }

        // Wait just one timeout for all to be produced
        auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);

        int i = 1;
        for (auto& fu : futures) {

            auto status = fu.wait_until(deadline);

            ASSERT_EQ(std::future_status::ready, status)
                << "Timed out reading response to produce batch " << i << " of " << num_batches_in_pipeline;

            auto decoder = fu.get();

            log()->debug() << "decoder cursor before resp read: " << decoder.get_cursor() << " for batch " << i;

            proto::ProduceResponse resp;

            decoder.io(resp);

            log()->debug() << "decoder cursor after resp read: " << decoder.get_cursor() << " for batch " << i;

            ASSERT_TRUE(decoder.ok()) << decoder.err_str();

            EXPECT_EQ(1ul,                  resp.topics.size()) << " for batch " << i;
            EXPECT_EQ("test",               resp.topics[0].name) << " for batch " << i;
            EXPECT_EQ(1ul,                  resp.topics[0].partitions.size()) << " for batch " << i;
            EXPECT_EQ(0,                    resp.topics[0].partitions[0].partition_id) << " for batch " << i;
            EXPECT_EQ(expected_err,         resp.topics[0].partitions[0].err_code) << " for batch " << i;
            ++i;
        }
    }

    boost::asio::io_service                         io_service_;
    std::auto_ptr<boost::asio::io_service::work>    work_;
    std::thread                                     asio_thread_;

    proto::MetadataResponse                         meta_;
    proto::Broker*                                  test_0_leader_;
    proto::Broker*                                  test_0_not_leader_;
    proto::Broker*                                  test_0_not_replica_;

    std::unique_ptr<MessageSet>                     ms_;
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

    err = b->sync_call(rq, resp, 1000);
    ASSERT_FALSE(err)
        << "Request failed: " << err.message();

    size_t broker_num = (size_t)get_env_int("KAFKA_BROKER_NUM");

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
    EXPECT_EQ(1ul, resp.topics.size());
    EXPECT_EQ("test", resp.topics[0].name);
    EXPECT_EQ(8ul, resp.topics[0].partitions.size());

    // Partitions should have 2 replicas
    EXPECT_EQ(2ul, resp.topics[0].partitions[0].replicas.size());

    // Partitions should have 2 in sync replicas
    EXPECT_EQ(2ul, resp.topics[0].partitions[0].isr.size());
}


TEST_F(BrokerTest, ProduceBatchUncompressed)
{
    ASSERT_NO_FATAL_FAILURE(SetUpProduce());
    test_connect_and_send_message_set(*test_0_leader_,0, kafka_error::NoError);
}

TEST_F(BrokerTest, ProduceBatchNonLeader)
{
    ASSERT_NO_FATAL_FAILURE(SetUpProduce());
    test_connect_and_send_message_set(*test_0_not_leader_, 0, kafka_error::NotLeaderForPartition);
}

TEST_F(BrokerTest, ProduceBatchNonReplica)
{
    ASSERT_NO_FATAL_FAILURE(SetUpProduce());
    test_connect_and_send_message_set(*test_0_not_replica_, 0, kafka_error::UnknownTopicOrPartition);
}

TEST_F(BrokerTest, ProduceBatchGZIP)
{
    ASSERT_NO_FATAL_FAILURE(SetUpProduce());
    ms_->push("Hello World. This is extra message for GZIP batch", "");
    ms_->set_compression(COMP_GZIP);
    test_connect_and_send_message_set(*test_0_leader_, 0, kafka_error::NoError);
}

TEST_F(BrokerTest, ProduceBatchSnappy)
{
    ASSERT_NO_FATAL_FAILURE(SetUpProduce());
    ms_->push("Hello World. This is extra message for Snappy batch", "");
    ms_->set_compression(COMP_Snappy);
    test_connect_and_send_message_set(*test_0_leader_, 0, kafka_error::NoError);
}

TEST_F(BrokerTest, ProduceBatchPipelined)
{
    ASSERT_NO_FATAL_FAILURE(SetUpProduce());
    ms_->push("Hello World. This is extra message for Pipelined batch", "");
    ms_->set_compression(COMP_None);
    test_connect_and_send_message_set(*test_0_leader_, 0, kafka_error::NoError, 3);
}

// All of the above can be verified it's really doing what is expected manually via:
//  1. check the test run output when DEBUG logs are on and see that first two sends result in something like
//       handle_write sent OK (370 bytes written)
//     while GZIP and snappy send fewer bytes:
//       handle_write sent OK (227 bytes written)
//       handle_write sent OK (259 bytes written)
//  2. Verify using `./kafka-simple-consumer-shell.sh --broker-list=192.168.59.103:9092 --topic test --partition 0`
//     that the messages are readable and sent as expected. They also decode compressed ones transparently (hence differnt messages in those)
// Note that the end-to-end integration tests in producer_client_test.cpp automate some of that too.
// these producing tests are mostly useful for pinpointing specific issues in simpler case than when testing the whole
// client too.


// Recreate segfault bug caused by Producer Client meta fetch allowing RPC call to be attermpted on a closed broker object
TEST_F(BrokerTest, NoClosedBrokerSegfault)
{
    // Connect to unroutable IP
    boost::shared_ptr<Broker> b(new Broker(io_service_, "192.0.2.0", get_env_int("KAFKA_1_PORT"), "test"));

    b->set_connect_timeout(10);

    auto ec = b->connect();

    EXPECT_TRUE((bool)ec);

    // Try making RPC call anyway, should fail, should not segfault
    proto::TopicMetadataRequest rq;
    proto::MetadataResponse resp;

    ec = b->sync_call(rq, resp, 100);

    // Should fail with network error
    EXPECT_EQ(synkafka_error::network_fail, ec)
        << "Expected timout err, got: " << ec.message();

    // This test trivially passes even without fix, but it also segfaults after this assertion when things start being destructed and RPC
    // callback fire.
    // Hmm Heisenbug alert - this appears to work just fine in GDB without fix, but fails consistently outside of debugger.
}

