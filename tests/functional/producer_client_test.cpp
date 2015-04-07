#include "gtest/gtest.h"

#include <cstdlib>
#include <memory>
#include <string>

#include "synkafka.h"

#include "test_cluster.h"

using namespace synkafka;

class ProducerClientTest : public ::testing::Test
{

protected:
    virtual void SetUp()
    {
        broker_config_ = get_env_string("KAFKA_1_HOST")
                + ":" + std::to_string(get_env_int("KAFKA_1_PORT"))
            + "," + get_env_string("KAFKA_2_HOST")
                + ":" + std::to_string(get_env_int("KAFKA_2_PORT"))
            + "," + get_env_string("KAFKA_3_HOST")
                + ":" + std::to_string(get_env_int("KAFKA_3_PORT"));

        client_.reset(new ProducerClient(broker_config_));
    }

    virtual void TearDown()
    {
    }

    std::string broker_config_;
    std::unique_ptr<ProducerClient> client_;
};


TEST_F(ProducerClientTest, PartitionAvailability)
{
    auto ec = client_->check_topic_partition_leader_available("test", 0);

    EXPECT_FALSE(ec);

    // Non existent topic
    ec = client_->check_topic_partition_leader_available("foobar", 0);

    EXPECT_EQ(kafka_error::UnknownTopicOrPartition, ec);

    // Valid topic, non existent partition
    ec = client_->check_topic_partition_leader_available("test", 128);

    EXPECT_EQ(kafka_error::UnknownTopicOrPartition, ec);

    // Disconnect kafkas....
    std::system("cd tests/functional && docker-compose -p synkafka kill");

    // Now even the valid one shoud fail with network error
    // Except that if we hit same broker as before, it will seem OK since we don't actually
    // send a message. We *could* test a different good partition here but the mapping is not
    // fixed and so test would ne non-deterministic based on whether or not leader happened to be the same
    // as leader for partition 0 above.
    // Instead we'll try get another one that is unknown, which will force a new meta data refresh which should
    // fail with network error, we should be told that network error occured NOT just that the partition is unknown
    ec = client_->check_topic_partition_leader_available("foobar", 98);

    EXPECT_EQ(synkafka_error::network_fail, ec)
        << "Got error: " << ec.message();

    std::system("cd tests/functional && docker-compose -p synkafka start");

    // And work again
    ec = client_->check_topic_partition_leader_available("test", 0);

    ASSERT_FALSE(ec);
}
