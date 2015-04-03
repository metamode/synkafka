#include "gtest/gtest.h"

#include <string>
#include <tuple>
#include <vector>

#include "protocol.h"
#include "slice.h"
#include "synkafka.h"

using namespace synkafka;


TEST(ProducerClient, ParseBrokerString)
{
    std::vector<std::tuple<std::string, std::deque<proto::Broker>>> cases = {
        {std::string{"localhost:1234"}, std::deque<proto::Broker>{proto::Broker{0, "localhost", 1234}}},
        {std::string{"localhost"}, std::deque<proto::Broker>{proto::Broker{0, "localhost", 9092}}},
        {std::string{"kafka01,kafka02,kafka03:9000"}, std::deque<proto::Broker>{proto::Broker{0, "kafka01", 9092}, proto::Broker{0, "kafka02", 9092}, proto::Broker{0, "kafka03", 9000}}},
    };

    for (auto& t : cases) {
        auto in = std::get<0>(t);
        auto ex = std::get<1>(t);

        auto out = ProducerClient::string_to_brokers(in);

        EXPECT_EQ(ex.size(), out.size());
        if (ex.size() != out.size()) {
            continue;
        }

        int i = 0;
        for (auto&b : out) {
            EXPECT_EQ(ex[i].host, b.host) << "Failed on broker " << i + 1;
            EXPECT_EQ(ex[i].port, b.port) << "Failed on broker " << i + 1;
            ++i;
        }
    }

}