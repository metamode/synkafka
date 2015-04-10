#include "gtest/gtest.h"

#include <ctime>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <list>
#include <memory>
#include <regex>
#include <string>
#include <thread>
#include <vector>

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

        client_.reset(new ProducerClient(broker_config_, 4));
    }

    virtual void TearDown()
    {
    }

    MessageSet make_message_set() 
    {
        MessageSet messages;

        // Pick a random seed for messages to that we are sure the ones we read are for this test run not
        // a previous one...
        auto now = std::chrono::high_resolution_clock::now();
        std::string now_str = std::to_string(now.time_since_epoch().count());

        for (int i = 0; i < 10; ++i) {
            messages.push(now_str + ": This is a test message number "+std::to_string(i), "Key "+std::to_string(i), true);
        }

        return messages;
    }

    std::list<std::string> get_last_n_messages(const std::string& topic, int partition, int n)
    {
        std::string base_cmd = "cd tests/functional && ./kafka-simple-consumer-shell.sh --broker-list="
                                + broker_config_
                                + " --topic test --partition " + std::to_string(partition)
                                + " --no-wait-at-logend --max-wait-ms=1";

        std::regex last_line_re("Terminating\\. .* at offset (\\d+)");

        // First get final offset
        auto lines = exec(base_cmd + " --offset -1");

        int last_offset;

        // Parse offset from final line
        std::smatch sm;

        if (std::regex_match(lines.back(), sm, last_line_re) && sm.size() == 2) {
            last_offset = std::atoi(sm[1].str().c_str());
        } else {
            throw std::runtime_error("Output of kafka-simple-consumer-shell.sh isn't what we expected");
        }

        // Now get last n from before offset
        if (last_offset < 0) {
            last_offset = 0;
        }

        lines = exec(base_cmd + " --offset " + std::to_string(last_offset - n));

        // Remove trailing line...
        lines.pop_back();

        return lines;
    }

    std::list<std::string> exec(const std::string& cmd)
    {
        FILE* pipe = popen(cmd.c_str(), "r");
        std::list<std::string> lines;

        char buf[1024];

        while (!feof(pipe)) {
            // Breaks if messages contain raw binary, but that is an error case anyway - means
            // out tests wrote junk
            if (fgets(buf, 1024, pipe) != nullptr) {
                size_t len = strlen(buf);
                if (len && buf[len-1] == '\n') {
                    --len;
                }
                lines.emplace_back(buf, strlen(buf) - 1);
            }
        }

        pclose(pipe);

        return lines;
    }

    std::string broker_config_;
    std::shared_ptr<ProducerClient> client_;
};


TEST_F(ProducerClientTest, PartitionAvailability)
{
    log()->debug("Existant topic/partition");

    auto ec = client_->check_topic_partition_leader_available("test", 0);

    ASSERT_FALSE(ec) << ec.message();


    log()->debug("Non-existant topic/partition");
    ec = client_->check_topic_partition_leader_available("foobar", 0);

    ASSERT_EQ(kafka_error::UnknownTopicOrPartition, ec);

    
    log()->debug("Valid topic, non-existant partition");
    ec = client_->check_topic_partition_leader_available("test", 128);

    ASSERT_EQ(kafka_error::UnknownTopicOrPartition, ec);

    log()->debug("Disconnected brokers");
    std::system("cd tests/functional && docker-compose -p synkafka kill");

    // Now even the valid one shoud fail with network error
    // Except that if we hit same broker as before, it will seem OK since we don't actually
    // send a message. We *could* test a different good partition here but the mapping is not
    // fixed and so test would ne non-deterministic based on whether or not leader happened to be the same
    // as leader for partition 0 above.
    // Instead we'll try get another one that is unknown, which will force a new meta data refresh which should
    // fail with network error, we should be told that network error occured NOT just that the partition is unknown
    ec = client_->check_topic_partition_leader_available("foobar", 98);

    // bring back cluster before we assert so we don't leave it broken for next test run if we fail
    std::system("cd tests/functional && docker-compose -p synkafka start");

    ASSERT_EQ(synkafka_error::network_fail, ec)
        << "Got error: " << ec.message();

    // Fudgy... if we try to reconnect too soon, it seems to fail - kafka brokers take a while to boot or something
    // after docker container is started.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    log()->debug("Reconnected brokers");
    ec = client_->check_topic_partition_leader_available("test", 0);

    ASSERT_FALSE(ec) << ec.message();
}

TEST_F(ProducerClientTest, BasicProducing)
{
    auto m = make_message_set();
    auto ec = client_->produce("test", 0, m);

    EXPECT_FALSE(ec);

    // This breaks if test kafka has any other writes to it concurrently with this process
    // woud also break if we ever try parallelize or run multiple tests at same time.
    // For now though it's a good sanity check in standalone test environment.
    auto messages = m.get_messages();
    auto lines = get_last_n_messages("test", 0, messages.size());
    ASSERT_EQ(messages.size(), lines.size())
        << "This should be true if the batch we just produced was the last one and was NOT compressed."
        << " If it *was* compressed of if we failed to produce and some other compressed batch is in last N"
        << " then the offset given will be wrong - kafka will return whole batch as a single offset apparently";
    int i = 0;
    for (auto& line : lines) {
        EXPECT_EQ(messages[i].value.str(), line);
        ++i;
    }
}

TEST_F(ProducerClientTest, GZIPProducing)
{
    auto m = make_message_set();
    m.set_compression(COMP_GZIP);

    auto ec = client_->produce("test", 0, m);

    EXPECT_FALSE(ec);

    // This breaks if test kafka has any other writes to it concurrently with this process
    // woud also break if we ever try parallelize or run multiple tests at same time.
    // For now though it's a good sanity check in standalone test environment.
    auto messages = m.get_messages();
    auto lines = get_last_n_messages("test", 0, 1); // Only fetch last 1 offset which shuld be compresed message with all 10 items
                                                    // this is not how I understand kafka offsets to work but seems to be how the simple
                                                    // consumer actually works in practice
    ASSERT_EQ(messages.size(), lines.size());
    int i = 0;
    for (auto& line : lines) {
        EXPECT_EQ(messages[i].value.str(), line);
        ++i;
    }
}

TEST_F(ProducerClientTest, SnappyProducing)
{
    auto m = make_message_set();
    m.set_compression(COMP_Snappy);

    auto ec = client_->produce("test", 0, m);

    EXPECT_FALSE(ec);

    // This breaks if test kafka has any other writes to it concurrently with this process
    // woud also break if we ever try parallelize or run multiple tests at same time.
    // For now though it's a good sanity check in standalone test environment.
    auto messages = m.get_messages();
    auto lines = get_last_n_messages("test", 0, 1); // Only fetch last 1 offset which shuld be compresed message with all 10 items
                                                    // this is not how I understand kafka offsets to work but seems to be how the simple
                                                    // consumer actually works in practice
    ASSERT_EQ(messages.size(), lines.size());
    int i = 0;
    for (auto& line : lines) {
        EXPECT_EQ(messages[i].value.str(), line);
        ++i;
    }
}

TEST_F(ProducerClientTest, ParallelProduce)
{
    // Run a separate thread for each partition all producing constantly for 5 seconds
    std::vector<std::thread> threads(8);

    std::atomic<uint64_t> batches_produced(0);

    for (int i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([&](int partition){
            auto start = std::chrono::high_resolution_clock::now();
            auto now = std::chrono::high_resolution_clock::now();
            auto m = make_message_set();
            do
            {
                auto ec = client_->produce("test", partition, m);

                ASSERT_FALSE(ec)
                    << "Produce failed with: " << ec.message()
                    << " from partition thread: " << partition;
                ++batches_produced;

                //std::this_thread::sleep_for(std::chrono::milliseconds(500));

                now = std::chrono::high_resolution_clock::now();
            } while ((now - start) < std::chrono::seconds(5));

        }, i);
    }    

    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }

    std::cout << "\t> Batches produced: " << batches_produced << std::endl;
}

// TODO
// - test error cases
//  - kill leader for partition, retry for up to a minute every second until failover
//  - kill replica and require 2 acks, ensure times out


