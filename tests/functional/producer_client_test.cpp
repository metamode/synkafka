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
#include "log.h"

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

    MessageSet make_message_set(size_t n = 10)
    {
        MessageSet messages;

        // Pick a random seed for messages to that we are sure the ones we read are for this test run not
        // a previous one...
        auto now = std::chrono::high_resolution_clock::now();
        std::string now_str = std::to_string(now.time_since_epoch().count());

        for (size_t i = 0; i < n; ++i) {
            messages.push(now_str + ": This is a test message number "+std::to_string(i), "Key "+std::to_string(i), true);
        }

        return messages;
    }

    std::list<std::string> get_last_n_messages(const std::string& topic, int partition, int n, int attempt = 1)
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
            if (attempt < 3) {
                // kafka test cluster is a little flaky - sometimes this script faild to get meta
                // even when cluster seems fine. So wait a bit and retry a couple of times...
                std::this_thread::sleep_for(std::chrono::milliseconds(500 * attempt));
                return get_last_n_messages(topic, partition, n, attempt + 1);
            }
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

    bool check_with_retry(const std::string& topic, int32_t partition, int retry_for_seconds)
    {
        auto now = std::chrono::system_clock::now();
        auto deadline = now + std::chrono::seconds(retry_for_seconds);
        int attempts = 0;
        int32_t new_leader_id = 9999;

        while (now < deadline) {
            auto ec = client_->check_topic_partition_leader_available("test", 0, &new_leader_id);

            if (ec) {
                ++attempts;
                log()->debug("Failed to get leader for (test, 0) on attempt ") << attempts;
            } else {
                log()->debug("Got new leader for (test, 0) on attempt ") << attempts
                    << ", new leader is node " << new_leader_id;

                EXPECT_NE(9999, new_leader_id);

                return true;
            }

            std::this_thread::sleep_for(std::chrono::seconds(5));

            now = std::chrono::system_clock::now();
        }
        return false;
    }

    std::list<std::string> exec(const std::string& cmd)
    {
        FILE* pipe = popen(cmd.c_str(), "r");
        std::list<std::string> lines;

        char buf[4096];

        while (!feof(pipe)) {
            // Breaks if messages contain raw binary, but that is an error case anyway - means
            // our tests wrote junk, also breaks if line is longer that buffer. Oh well just ensure our tests
            // don't write messages that long...
            if (fgets(buf, 4096, pipe) != nullptr) {
                size_t len = strlen(buf);
                if (len && buf[len-1] == '\n') {
                    --len;
                }
                lines.emplace_back(buf, len);
            }
        }

        pclose(pipe);

        return lines;
    }

    std::string broker_config_;
    std::shared_ptr<ProducerClient> client_;
};


TEST_F(ProducerClientTest, BasicProducing)
{
    auto m = make_message_set();
    auto ec = client_->produce("test", 0, m);

    EXPECT_FALSE(ec) << ec.message();

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

    EXPECT_FALSE(ec) << ec.message();

    // This breaks if test kafka has any other writes to it concurrently with this process
    // woud also break if we ever try parallelize or run multiple tests at same time.
    // For now though it's a good sanity check in standalone test environment.
    auto messages = m.get_messages();
    auto lines = get_last_n_messages("test", 0, 1); // Only fetch last 1 offset which shuld be compressed message with all 10 items
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

    EXPECT_FALSE(ec) << ec.message();

    // This breaks if test kafka has any other writes to it concurrently with this process
    // woud also break if we ever try parallelize or run multiple tests at same time.
    // For now though it's a good sanity check in standalone test environment.
    auto messages = m.get_messages();
    auto lines = get_last_n_messages("test", 0, 1); // Only fetch last 1 offset which shuld be compressed message with all 10 items
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

    // Disable debug logging since it's super spammy on this test
    auto level = log()->level();
    if (level <= spdlog::level::debug) {
        log()->debug("Disabling spammy debug output");
        log()->set_level(spdlog::level::info);
    }

    size_t batch_size = 1000;

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([&](int partition){
            auto start = std::chrono::high_resolution_clock::now();
            auto now = std::chrono::high_resolution_clock::now();
            // 1000 messages in a batch to make it non trivial/more realistic
            auto m = make_message_set(batch_size);
            m.set_compression(COMP_GZIP);
            do
            {
                auto ec = client_->produce("test", partition, m);

                ASSERT_FALSE(ec)
                    << "Produce failed with: " << ec.message()
                    << " from partition thread: " << partition;
                ++batches_produced;

                //log()->info("Produced batch ") << batches_produced << " to partition " << partition;

                //std::this_thread::sleep_for(std::chrono::milliseconds(2000));

                now = std::chrono::high_resolution_clock::now();
            } while ((now - start) < std::chrono::seconds(5));

        }, i);
    }

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }

    log()->set_level(level);
    log()->debug("Enabled spammy debug output");

    std::cout << "\t> Batches produced: " << batches_produced << ", messages: " << (batch_size * batches_produced) << std::endl;
}

TEST_F(ProducerClientTest, ProduceRebalance)
{
    auto m = make_message_set();

    // Produce one batch to be sure leader is up before
    auto ec = client_->produce("test", 0, m);
    ASSERT_FALSE(ec) << ec.message();

    // Figure out which node it went to
    int32_t leader_id = 9999;
    ec = client_->check_topic_partition_leader_available("test", 0, &leader_id);

    ASSERT_FALSE(ec) << ec.message();
    ASSERT_NE(9999, leader_id);

    log()->debug("Leader for (test, 0) before is ") << leader_id;

    // Now force the cluster to rebalance
    // Have to write new assignment as a JSON file for command...
    // write a temp file...
    // Pick first broker that isn't leader (assume we have more than one and the are consecutive from 9091)
    int32_t new_leader_id = 9091;
    while (new_leader_id == leader_id) {
        new_leader_id++;
    }
    // Make old leader the secondary (assumes 2 replicas)
    auto new_replicas = std::to_string(new_leader_id) + "," + std::to_string(leader_id);

    log()->debug("Moving replica set for (test, 0) to [") << new_leader_id << ", " << leader_id << "]";

    std::string reassign_cmd = "cd tests/functional && ./reassign-partitions.sh test 0 "
        + std::to_string(new_leader_id) + " " + std::to_string(leader_id);

    auto ret = std::system((reassign_cmd + " --execute").c_str());

    ASSERT_EQ(0, ret) << "Partition re-assign command failed";

    // Wait for brokers to switch assignment (it's async)
    int attempts = 0;
    bool success = false;
    do {
        auto lines = exec(reassign_cmd + " --verify");

        if (lines.size() && lines.back() == "Reassignment of partition [test,0] completed successfully") {
            success = true;
            log()->debug("Verified replica move completed after ") << attempts+1 << " attempts";
            break;
        }

        ++attempts;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    } while (attempts < 20);

    ASSERT_TRUE(success) << "Failed to reassign or verify that assign worked after " << attempts+1 << " attempts";

    // So kafka even after meta data is updated in Zookeeper still doesn't actually decomission old leader and elect new one
    // until some time later it seems. Occasionally I've observed that to be several minutes even on my little test cluster
    // ... which kinda sucks
    // So best we can do is keep trying to produce for next 5 mins.
    // UNtil it happens we should still be producing to old leader
    // once it happens we should get an error (which should internally trigger meta update)
    // we should then recover from that in a timely way.
    // If we don't fail to produce to old master in that whole time then test isn't really working as it's not testing
    // actual reassignment behaviour.
    attempts = 0;
    success = false;
    do {
        std::this_thread::sleep_for(std::chrono::seconds(5));

        auto m2 = make_message_set(1);

        ec = client_->produce("test", 0, m2);

        // Should either succeed (not completed reassignment yet)
        // or fail with NotLeaderForPartition
        if (ec) {
            ASSERT_EQ(kafka_error::NotLeaderForPartition, ec)
                << "Expected NotLeaderForPartition failure, got: " << ec.message();
            success = true;
            break;
        }

        ++attempts;
    } while (attempts < 300);

    ASSERT_TRUE(success) << "Failed to complete reassign to new master after " << attempts+1 << " attempts";

    // Then if we keep trying for long enough
    // we should eventually discover a new broker
    // and be able to continue producing
    EXPECT_TRUE(check_with_retry("test", 0, 30));

    // Now produce should work
    ec = client_->produce("test", 0, m);
    EXPECT_FALSE(ec) << ec.message();
}

// Tests that mess with cluster go last.
// crude but no (sane) amount of sleeping can reliably
// gaurantee tests running after we fail nodes and bring them back actually pass
// This guarantees nothing either, just makes it more likely in practice that cluster had
// time to heal between runs.

TEST_F(ProducerClientTest, ProduceFailover)
{
    auto m = make_message_set();

    // Produce one batch to be sure leader is up before
    auto ec = client_->produce("test", 0, m);
    ASSERT_FALSE(ec) << ec.message();

    // Figure out which node it went to
    int32_t leader_id = 9999;
    ec = client_->check_topic_partition_leader_available("test", 0, &leader_id);

    ASSERT_FALSE(ec) << ec.message();
    ASSERT_NE(9999, leader_id);

    log()->debug("Leader for (test, 0) before is ") << leader_id;

    // Now we must kill just that nodeid. We figure it out based on knowledge of config
    // if that changes, this test should be updated.
    // Currently nodeid is set to be same as public container port which is set to 9091,9092,9093
    // for brokers 1,2,3 respectively.
    std::string broker_name = "kafka" + std::to_string(leader_id - 9090);

    log()->debug("Disconnecting broker " + broker_name);
    std::system(std::string("cd tests/functional && docker-compose -p synkafka kill " + broker_name).c_str());

    // We should fail to connect on next attempt
    ec = client_->produce("test", 0, m);
    EXPECT_FALSE(!ec)
        << "Expected network failure, got: " << ec.message();

    // Then if we keep trying for long enough (say 30 seconds - shorter would fail often on my test setup)
    // we should eventually discover a new broker
    // and be able to continue producing
    EXPECT_TRUE(check_with_retry("test", 0, 30));

    // Now produce should work
    ec = client_->produce("test", 0, m);
    EXPECT_FALSE(ec) << ec.message();

    log()->debug("Reconnecting broker " + broker_name);
    std::system(std::string("cd tests/functional && docker-compose -p synkafka start " + broker_name).c_str());
}

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
    std::this_thread::sleep_for(std::chrono::seconds(10));

    log()->debug("Reconnected brokers");
    ec = client_->check_topic_partition_leader_available("test", 0);

    ASSERT_FALSE(ec) << ec.message();
}
