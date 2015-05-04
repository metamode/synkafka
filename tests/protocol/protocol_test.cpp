
#include "gtest/gtest.h"

#include "protocol.h"
#include "message_set.h"

#include "slice.h"
#include "buffer.h"

using namespace synkafka;

TEST(Protocol, RequestHeaderCodec)
{
    proto::RequestHeader h = {ApiKey::ProduceRequest, 0, 1234, "tester"};

    slice expected("\x00\x00\x00\x10" // i32 Length prefix of whole packet (16)
                   "\x00\x00" // ApiProduceRequest
                   "\x00\x00" // Version 0
                   "\x00\x00\x04\xd2" // 1234
                   "\x00\x06" // 6 bytes of string
                   "tester" // string client id
                  ,20);

    PacketEncoder pe(128);

    pe.io(h);
    ASSERT_TRUE(pe.ok()) << pe.err_str();

    auto encoded = pe.get_as_slice(true);
    ASSERT_EQ(expected.size(), encoded.size());
    ASSERT_EQ(0, expected.compare(encoded))
        << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
        << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

    // Read it back
    proto::RequestHeader h2;

    // Remove the length prefix from the encoded buffer
    encoded.advance(sizeof(int32_t));

    PacketDecoder pd(buffer_from_string(encoded.str()));

    pd.io(h2);

    ASSERT_TRUE(pd.ok()) << pd.err_str();

    ASSERT_EQ(h.api_key, h2.api_key);
    ASSERT_EQ(h.api_version, h2.api_version);
    ASSERT_EQ(h.correlation_id, h2.correlation_id);
    ASSERT_EQ(h.client_id, h2.client_id);
}

TEST(Protocol, ResponseHeaderCodec)
{
    proto::ResponseHeader h = {1234};

    slice expected("\x00\x00\x00\x04" // i32 Length prefix of whole packet (16)
                   "\x00\x00\x04\xd2" // 1234
                  ,8);

    PacketEncoder pe(128);

    pe.io(h);
    ASSERT_TRUE(pe.ok()) << pe.err_str();

    auto encoded = pe.get_as_slice(true);
    ASSERT_EQ(expected.size(), encoded.size());
    ASSERT_EQ(0, expected.compare(encoded))
        << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
        << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

    // Read it back
    proto::ResponseHeader h2;

    // Remove the length prefix from the encoded buffer
    encoded.advance(sizeof(int32_t));

    PacketDecoder pd(buffer_from_string(encoded.str()));

    pd.io(h2);

    ASSERT_TRUE(pd.ok()) << pd.err_str();

    ASSERT_EQ(h.correlation_id, h2.correlation_id);
}

// Message test cases
std::vector<std::tuple<MessageSet::Message, slice>> g_messageTestCases {
    std::make_tuple(MessageSet::Message{"", "test message", 0, COMP_None} // construct message with value only
                   ,slice("\xa6\xe0\xcd\x8d" // crc32
                          "\x00\x00" // Magic + attributes
                          "\xFF\xFF\xFF\xFF" // Null byte key (-1)
                          "\x00\x00\x00\x0c" // 12 byte value
                          "test message"
                         ,26
                         )
                   ),
    std::make_tuple(MessageSet::Message{"test key", "test message", 0, COMP_None} // construct message with value only
                   ,slice("\xb5\x55\x94\x26" // crc32
                            "\x00\x00" // Magic + attributes
                            "\x00\x00\x00\x08" // 8 byte key
                            "test key"
                            "\x00\x00\x00\x0c" // 12 byte value
                            "test message"
                           ,34
                           )
    ),
    std::make_tuple(MessageSet::Message{"", "", 0, COMP_None} // empty
                   ,slice("\xa7\xec\x68\x03" // crc32
                            "\x00\x00" // Magic + attributes
                            "\xFF\xFF\xFF\xFF" // Null byte key -1
                            "\xFF\xFF\xFF\xFF" // Null byte value -1
                           ,14
                           )
    ),
};

TEST(Protocol, MessageCodec)
{
    for (auto& test : g_messageTestCases) {
        auto m = std::get<0>(test);
        auto expected = std::get<1>(test);

        PacketEncoder pe(128);

        pe.io(m);
        ASSERT_TRUE(pe.ok()) << pe.err_str();

        // No length prefix that's well tested and not needed.
        auto encoded = pe.get_as_slice(false);
        ASSERT_EQ(expected.size(), encoded.size());
        ASSERT_EQ(0, expected.compare(encoded))
            << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
            << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

        PacketDecoder pd(buffer_from_string(encoded.str()));

        MessageSet::Message m2;
        pd.io(m2);
        ASSERT_TRUE(pd.ok()) << pd.err_str();

        EXPECT_EQ(m.key, m2.key);
        EXPECT_EQ(m.value, m2.value);
        EXPECT_EQ(m.compression_, m2.compression_);
    }
}

void add_messages(MessageSet& ms)
{
    for (auto& test : g_messageTestCases) {
        ms.push(std::move(std::get<0>(test)));
    }
}

void assert_same_messages(MessageSet& ms1, MessageSet& ms2)
{
    auto messages1 = ms1.get_messages();
    auto messages2 = ms2.get_messages();

    ASSERT_EQ(messages1.size(), messages2.size());

    int idx = 0;
    for (auto& m : messages2) {
        auto expected_m = messages1[idx];
        EXPECT_EQ(expected_m.key,         m.key)
            << "Expected: <" << expected_m.key.hex() << "> ("<< expected_m.key.size() << ")\n"
            << "Got:      <" << m.key.hex() << "> ("<< m.key.size() << ")";
        EXPECT_EQ(expected_m.value,     m.value)
            << "Expected: <" << expected_m.value.hex() << "> ("<< expected_m.value.size() << ")\n"
            << "Got:      <" << m.value.hex() << "> ("<< m.value.size() << ")";
        EXPECT_EQ(expected_m.offset,     m.offset);
        ++idx;
    }
}

TEST(Protocol, MessageSetCodec)
{
    // First encode above 3 messages with no compression
    std::stringstream expectedSS;

    expectedSS << std::string("\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                              "\x00\x00\x00\x1a"                  // 26 bytes for message 1
                             ,12
                             )
               << std::get<1>(g_messageTestCases[0]).str()
               << std::string("\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                              "\x00\x00\x00\x22"                  // 34 bytes for message 2
                             ,12
                             )
               << std::get<1>(g_messageTestCases[1]).str()
               << std::string("\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                              "\x00\x00\x00\x0e"                  // 14 bytes for message 3
                             ,12
                             )
               << std::get<1>(g_messageTestCases[2]).str();

    auto expectedStr = expectedSS.str();
    auto expected = slice(expectedStr);

    // Full encoded hex for messages set: (for manually generated expected compressed messages below)
    // 00000000000000000000001aa6e0cd8d0000ffffffff0000000c74657374206d657373616765
    // 000000000000000000000022b555942600000000000874657374206b65790000000c74657374
    // 206d65737361676500000000000000000000000ea7ec68030000ffffffffffffffff

    MessageSet ms;
    add_messages(ms);

    // Intentionall too small for message set to test buffer expansion happens correctly
    // note that this is long enough that one of the crc fields from message is written before
    // it expands
    PacketEncoder pe(30);

    pe.io(ms);
    ASSERT_TRUE(pe.ok()) << pe.err_str();

    // No length prefix that's well tested and not needed.
    auto encoded = pe.get_as_slice(false);
    ASSERT_EQ(expected.size(), encoded.size());
    ASSERT_EQ(0, expected.compare(encoded))
        << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
        << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

    // Read it back
    PacketDecoder pd(buffer_from_string(encoded.str()));

    MessageSet ms2;
    pd.io(ms2);
    ASSERT_TRUE(pd.ok()) << pd.err_str();

    assert_same_messages(ms, ms2);
}

TEST(Protocol, MessageSetCodecGZIP)
{
    MessageSet ms;
    ms.set_compression(COMP_GZIP);
    add_messages(ms);

    // Gzipped message set from previous test
    slice expected("\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                   "\x00\x00\x00\x59"                   // 89 bytes for the wrapper message with gzip payload
                   "\xb4\x85\xd4\xfe" // CRC32
                   "\x00\x01" // magic and gzip attribute
                   "\xFF\xFF\xFF\xFF" // null key
                   "\x00\x00\x00\x4b" // 75 byte gzipped value
                   // Gzipped message set from previous test
                   "\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x63\x60"
                   "\x80\x03\xa9\x65\x0f\xce\xf6\x32\x30\xfc\x07\x02"
                   "\x20\x8f\xa7\x24\xb5\xb8\x44\x21\x37\xb5\xb8\x38"
                   "\x31\x3d\x15\xa1\x88\x41\x69\x6b\xe8\x14\x35\x30"
                   "\x8b\x03\xac\x22\x3b\xb5\x12\x8f\x6a\xbe\xe5\x6f"
                   "\x32\x98\x21\x66\x82\x00\x00\x20\x06\xcb\x11\x6e"
                   "\x00\x00\x00", 101);

    PacketEncoder pe(128);

    pe.io(ms);
    ASSERT_TRUE(pe.ok()) << pe.err_str();

    auto encoded = pe.get_as_slice(false);
    EXPECT_EQ(expected.size(), encoded.size());
    ASSERT_EQ(0, expected.compare(encoded))
        << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
        << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

    // Read it back
    MessageSet ms2;

    PacketDecoder pd(buffer_from_string(encoded.str()));

    pd.io(ms2);

    ASSERT_TRUE(pd.ok()) << pd.err_str();

    assert_same_messages(ms, ms2);
}

TEST(Protocol, MessageSetCodecSnappy)
{
    MessageSet ms;
    ms.set_compression(COMP_Snappy);
    add_messages(ms);

    // Gzipped message set from previous test
    slice expected("\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                   "\x00\x00\x00\x55"                   // 85 bytes for the wrapper message with snappy payload
                   "\x96\x0f\xbd\x42" // CRC32
                   "\x00\x02" // magic and snappy attribute
                   "\xFF\xFF\xFF\xFF" // null key
                   "\x00\x00\x00\x47" // 71 byte gzipped value
                   // Snappyed message set from previous test
                   "\x6e\x00\x00\x19\x01\x68\x1a\xa6\xe0\xcd\x8d\x00"
                   "\x00\xff\xff\xff\xff\x00\x00\x00\x0c\x74\x65\x73"
                   "\x74\x20\x6d\x65\x73\x73\x61\x67\x65\x19\x25\x14"
                   "\x00\x22\xb5\x55\x94\x26\x05\x10\x00\x08\x05\x22"
                   "\x08\x6b\x65\x79\x6a\x2e\x00\x38\x0e\xa7\xec\x68"
                   "\x03\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff", 97);

    PacketEncoder pe(128);

    pe.io(ms);
    ASSERT_TRUE(pe.ok()) << pe.err_str();

    auto encoded = pe.get_as_slice(false);
    EXPECT_EQ(expected.size(), encoded.size());
    ASSERT_EQ(0, expected.compare(encoded))
        << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
        << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

    // Read it back
    MessageSet ms2;

    PacketDecoder pd(buffer_from_string(encoded.str()));

    pd.io(ms2);

    ASSERT_TRUE(pd.ok()) << pd.err_str();

    assert_same_messages(ms, ms2);
}

TEST(Protocol, TopicMetadataRequestCodec)
{
    std::vector<std::tuple<proto::TopicMetadataRequest, slice>> test_cases = {
        std::make_tuple(proto::TopicMetadataRequest{{"one", "two", "three"}}
                       ,slice("\x00\x00\x00\x03"
                                 "\x00\x03""one"
                                 "\x00\x03""two"
                                 "\x00\x05""three"
                                ,21
                                )
                       ),
        std::make_tuple(proto::TopicMetadataRequest{{}}
                       ,slice("\x00\x00\x00\x00"
                               ,4
                               )
                       ),
        std::make_tuple(proto::TopicMetadataRequest{{"singleton"}}
                       ,slice("\x00\x00\x00\x01"
                                 "\x00\x09""singleton"
                               ,15
                               )
                       ),
    };

    for (auto& t : test_cases) {
        auto r = std::get<0>(t);
        auto expected = std::get<1>(t);

        PacketEncoder pe(128);

        pe.io(r);
        ASSERT_TRUE(pe.ok()) << pe.err_str();

        auto encoded = pe.get_as_slice(false);
        EXPECT_EQ(expected.size(), encoded.size());
        ASSERT_EQ(0, expected.compare(encoded))
            << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
            << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

        // Read it back
        proto::TopicMetadataRequest r2;

        PacketDecoder pd(buffer_from_string(encoded.str()));

        pd.io(r2);

        EXPECT_TRUE(pd.ok()) << pd.err_str();

        EXPECT_EQ(r.topic_names, r2.topic_names);
    }
}

TEST(Protocol, BrokerCodec)
{
    std::vector<std::tuple<proto::Broker, slice>> test_cases = {
        std::make_tuple(proto::Broker{1, "kafka01", 9092}
                       ,slice("\x00\x00\x00\x01" // node id
                                 "\x00\x07""kafka01"
                                 "\x00\x00\x23\x84"
                                ,17)
                       ),
    };

    for (auto& t : test_cases) {
        auto b = std::get<0>(t);
        auto expected = std::get<1>(t);

        PacketEncoder pe(128);

        pe.io(b);
        ASSERT_TRUE(pe.ok()) << pe.err_str();

        auto encoded = pe.get_as_slice(false);
        EXPECT_EQ(expected.size(), encoded.size());
        ASSERT_EQ(0, expected.compare(encoded))
            << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
            << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

        // Read it back
        proto::Broker b2;

        PacketDecoder pd(buffer_from_string(encoded.str()));

        pd.io(b2);

        EXPECT_TRUE(pd.ok()) << pd.err_str();

        EXPECT_EQ(b.node_id, b2.node_id);
        EXPECT_EQ(b.host, b2.host);
        EXPECT_EQ(b.port, b2.port);
    }
}

TEST(Protocol, MetadataResponseCodec)
{
    std::vector<std::tuple<proto::MetadataResponse, slice>> test_cases {
        std::make_tuple(proto::MetadataResponse{{{1, "kafka01", 9092},{2, "kafka02", 9092}} // brokers
                                               ,{{kafka_error::NoError
                                                 ,"foo" // Topic name
                                                 ,{{kafka_error::NoError
                                                     ,0 // partitionid
                                                     ,1 // leader
                                                     ,{1,2} // replicas
                                                     ,{1,2} // isr
                                                     }
                                                  ,{kafka_error::NoError
                                                     ,1 // partitionid
                                                     ,2 // leader
                                                     ,{2,1} // replicas
                                                     ,{2,1} // isr
                                                     }
                                                  ,{kafka_error::LeaderNotAvailable
                                                     ,2 // partitionid
                                                     ,3 // leader
                                                     ,{3,1} // replicas
                                                     ,{1} // isr
                                                     }
                                                  } // Partitions
                                                 }
                                                ,{kafka_error::NoError
                                                 ,"bar" // Topic name
                                                 ,{{kafka_error::NoError
                                                     ,0 // partitionid
                                                     ,1 // leader
                                                     ,{1,2} // replicas
                                                     ,{1,2} // isr
                                                     }
                                                  ,{kafka_error::NoError
                                                     ,1 // partitionid
                                                     ,2 // leader
                                                     ,{2,1} // replicas
                                                     ,{2} // isr
                                                     }
                                                  } // Partitions
                                                 }
                                                } // topics
                                               }
                       ,slice("\x00\x00\x00\x02" // 2 brokers in list
                                      "\x00\x00\x00\x01" // Broker 1 id
                                         "\x00\x07""kafka01" // broker 1 host
                                      "\x00\x00\x23\x84" // broker 1 port
                                      "\x00\x00\x00\x02" // Broker 2 id
                                         "\x00\x07""kafka02" // broker 2 host
                                      "\x00\x00\x23\x84" // broker 2 port
                                  "\x00\x00\x00\x02" // 2 topics in list
                                         "\x00\x00" // topic 1 err_code
                                         "\x00\x03""foo" // topic 1 name
                                         "\x00\x00\x00\x03" // 3 partitions in list
                                             "\x00\x00" // partition 1 err_code
                                             "\x00\x00\x00\x00" // partition 1 id
                                             "\x00\x00\x00\x01" // partition 1 leader
                                             "\x00\x00\x00\x02" // 2 elements in partition 1 replica list
                                                 "\x00\x00\x00\x01" // replica 1
                                                 "\x00\x00\x00\x02" // replica 2
                                             "\x00\x00\x00\x02" // 2 elements in partition 1 isr list
                                                 "\x00\x00\x00\x01" // replica 1
                                                 "\x00\x00\x00\x02" // replica 2
                                             "\x00\x00" // partition 2 err_code
                                             "\x00\x00\x00\x01" // partition 2 id
                                             "\x00\x00\x00\x02" // partition 2 leader
                                             "\x00\x00\x00\x02" // 2 elements in partition 2 replica list
                                                 "\x00\x00\x00\x02" // replica 1
                                                 "\x00\x00\x00\x01" // replica 2
                                             "\x00\x00\x00\x02" // 2 elements in partition 2 isr list
                                                 "\x00\x00\x00\x02" // replica 1
                                                 "\x00\x00\x00\x01" // replica 2
                                             "\x00\x05" // partition 3 err_code
                                             "\x00\x00\x00\x02" // partition 3 id
                                             "\x00\x00\x00\x03" // partition 3 leader
                                             "\x00\x00\x00\x02" // 2 elements in partition 3 replica list
                                                 "\x00\x00\x00\x03" // replica 1
                                                 "\x00\x00\x00\x01" // replica 2
                                             "\x00\x00\x00\x01" // 1 elements in partition 3 isr list
                                                 "\x00\x00\x00\x01" // replica 2
                                         "\x00\x00" // topic 2 err_code
                                         "\x00\x03""bar" // topic 2 name
                                         "\x00\x00\x00\x02" // 2 partitions in list
                                             "\x00\x00" // partition 1 err_code
                                             "\x00\x00\x00\x00" // partition 1 id
                                             "\x00\x00\x00\x01" // partition 1 leader
                                             "\x00\x00\x00\x02" // 2 elements in partition 1 replica list
                                                 "\x00\x00\x00\x01" // replica 1
                                                 "\x00\x00\x00\x02" // replica 2
                                             "\x00\x00\x00\x02" // 2 elements in partition 1 isr list
                                                 "\x00\x00\x00\x01" // replica 1
                                                 "\x00\x00\x00\x02" // replica 2
                                             "\x00\x00" // partition 2 err_code
                                             "\x00\x00\x00\x01" // partition 2 id
                                             "\x00\x00\x00\x02" // partition 2 leader
                                             "\x00\x00\x00\x02" // 2 elements in partition 2 replica list
                                                 "\x00\x00\x00\x02" // replica 1
                                                 "\x00\x00\x00\x01" // replica 2
                                             "\x00\x00\x00\x01" // 1 elements in partition 2 isr list
                                                 "\x00\x00\x00\x02" // replica 1
                                ,226
                                )
                       ),
    };

    for (auto& t : test_cases) {
        auto r = std::get<0>(t);
        auto expected = std::get<1>(t);

        PacketEncoder pe(256);

        pe.io(r);
        ASSERT_TRUE(pe.ok()) << pe.err_str();

        auto encoded = pe.get_as_slice(false);
        EXPECT_EQ(expected.size(), encoded.size());
        ASSERT_EQ(0, expected.compare(encoded))
            << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
            << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

        // Read it back
        proto::MetadataResponse r2;

        PacketDecoder pd(buffer_from_string(encoded.str()));

        pd.io(r2);

        EXPECT_TRUE(pd.ok()) << pd.err_str();

        EXPECT_EQ(r.brokers.size(), r2.brokers.size());
        EXPECT_EQ(r.topics.size(), r2.topics.size());

        int idx = 0;
        for (auto& broker : r2.brokers) {
            auto expected_broker = r.brokers[idx];
            EXPECT_EQ(expected_broker.node_id, broker.node_id);
            EXPECT_EQ(expected_broker.host, broker.host);
            EXPECT_EQ(expected_broker.port, broker.port);
            ++idx;
        }

        idx = 0;
        for (auto& topic : r2.topics) {
            auto expected_topic = r.topics[idx];
            EXPECT_EQ(expected_topic.err_code,             topic.err_code);
            EXPECT_EQ(expected_topic.name,                 topic.name);
            EXPECT_EQ(expected_topic.partitions.size(), topic.partitions.size());

            int j = 0;
            for (auto& partition : topic.partitions) {
                auto expected_partition = expected_topic.partitions[j];

                EXPECT_EQ(expected_partition.err_code,             partition.err_code);
                EXPECT_EQ(expected_partition.partition_id,         partition.partition_id);
                EXPECT_EQ(expected_partition.leader,             partition.leader);
                EXPECT_EQ(expected_partition.replicas,             partition.replicas);
                EXPECT_EQ(expected_partition.isr,                 partition.isr);

                ++j;
            }

            ++idx;
        }
    }
}

TEST(Protocol, ProduceRequestCodec)
{
    MessageSet ms1;
    ms1.push("test message", "");
    ms1.push("test message", "");


    std::vector<std::tuple<proto::ProduceRequest, slice>> test_cases {
        std::make_tuple(proto::ProduceRequest{1 // required acks
                                               ,1000 // timeout
                                               ,{proto::ProduceTopic{"foo"
                                                                    ,{proto::ProducePartition{0
                                                                                             ,ms1
                                                                                             }
                                                                     ,proto::ProducePartition{1
                                                                                             ,ms1
                                                                                              }
                                                                     }
                                                                    }
                                                }
                                               }

                       ,slice("\x00\x01" // required acks
                                 "\x00\x00\x03\xe8" // timeout
                                 "\x00\x00\x00\x01" // 1 topic in list
                                        "\x00\x03""foo" // topic 1 name
                                        "\x00\x00\x00\x02" // 2 partition batches in list
                                            "\x00\x00\x00\x00" // partition 1 id
                                            "\x00\x00\x00\x4c" // partition 1 message set size (76 bytes)
                                                "" // Message set begins
                                                "\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                                             "\x00\x00\x00\x1a"                   // 26 bytes for message
                                                 "\xa6\xe0\xcd\x8d" // crc32
                                                 "\x00\x00" // Magic + attributes
                                                 "\xFF\xFF\xFF\xFF" // Null byte key (-1)
                                                 "\x00\x00\x00\x0c" // 12 byte value
                                                 "test message"
                                                "\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                                             "\x00\x00\x00\x1a"                   // 26 bytes for message
                                                 "\xa6\xe0\xcd\x8d" // crc32
                                                 "\x00\x00" // Magic + attributes
                                                 "\xFF\xFF\xFF\xFF" // Null byte key (-1)
                                                 "\x00\x00\x00\x0c" // 12 byte value
                                                 "test message"
                                            "\x00\x00\x00\x01" // partition 2 id
                                            "\x00\x00\x00\x4c" // partition 2 message set size (76 bytes)
                                                "" // Message set begins
                                                "\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                                             "\x00\x00\x00\x1a"                   // 26 bytes for message
                                                 "\xa6\xe0\xcd\x8d" // crc32
                                                 "\x00\x00" // Magic + attributes
                                                 "\xFF\xFF\xFF\xFF" // Null byte key (-1)
                                                 "\x00\x00\x00\x0c" // 12 byte value
                                                 "test message"
                                                "\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                                             "\x00\x00\x00\x1a"                   // 26 bytes for message
                                                 "\xa6\xe0\xcd\x8d" // crc32
                                                 "\x00\x00" // Magic + attributes
                                                 "\xFF\xFF\xFF\xFF" // Null byte key (-1)
                                                 "\x00\x00\x00\x0c" // 12 byte value
                                                 "test message"
                                ,187
                                )
                       ),
    };

    for (auto& t : test_cases) {
        auto r = std::get<0>(t);
        auto expected = std::get<1>(t);

        PacketEncoder pe(256);

        pe.io(r);
        ASSERT_TRUE(pe.ok()) << pe.err_str();

        auto encoded = pe.get_as_slice(false);
        EXPECT_EQ(expected.size(), encoded.size());
        ASSERT_EQ(0, expected.compare(encoded))
            << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
            << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

        // Read it back
        proto::ProduceRequest r2;

        PacketDecoder pd(buffer_from_string(encoded.str()));

        pd.io(r2);

        EXPECT_TRUE(pd.ok()) << pd.err_str();

        EXPECT_EQ(r.required_acks, r2.required_acks);
        EXPECT_EQ(r.timeout, r2.timeout);
        EXPECT_EQ(r.topics.size(), r2.topics.size());

        int idx = 0;
        for (auto& topic : r2.topics) {
            auto expected_topic = r.topics[idx];
            EXPECT_EQ(expected_topic.name, topic.name);
            EXPECT_EQ(expected_topic.partitions.size(), topic.partitions.size());

            int jdx = 0;
            for (auto& partition : topic.partitions) {
                auto expected_partition = expected_topic.partitions[jdx];

                EXPECT_EQ(expected_partition.partition_id, partition.partition_id);

                assert_same_messages(expected_partition.messages, partition.messages);

                ++jdx;
            }

            ++idx;
        }
    }
}

TEST(Protocol, ProduceResponseCodec)
{
    std::vector<std::tuple<proto::ProduceResponse, slice>> test_cases = {
        std::make_tuple(proto::ProduceResponse{{proto::ProduceResponseTopic{"foo"
                                                                           ,{proto::ProduceResponsePartition{0, kafka_error::NoError, 123456789}
                                                                            ,proto::ProduceResponsePartition{1, kafka_error::NoError, 145236589}
                                                                            ,proto::ProduceResponsePartition{2, kafka_error::NoError, 135426589}
                                                                            }
                                                                           }
                                               ,proto::ProduceResponseTopic{"bar"
                                                                           ,{proto::ProduceResponsePartition{0, kafka_error::NoError, 123456789}
                                                                            ,proto::ProduceResponsePartition{1, kafka_error::NotLeaderForPartition, 0}
                                                                            }
                                                                           }
                                               }
                                              }
                        ,slice("\x00\x00\x00\x02" // 2 topics
                                      "\x00\x03""foo" // topic 1 name
                                      "\x00\x00\x00\x03" // 3 partitions
                                          "\x00\x00\x00\x00" // partition 1 id
                                          "\x00\x00" // partition 1 err_code
                                          "\x00\x00\x00\x00\x07\x5b\xcd\x15" // Null Offset
                                          "\x00\x00\x00\x01" // partition 2 id
                                          "\x00\x00" // partition 1 err_code
                                          "\x00\x00\x00\x00\x08\xa8\x22\x6d" // Null Offset
                                          "\x00\x00\x00\x02" // partition 3 id
                                          "\x00\x00" // partition 1 err_code
                                          "\x00\x00\x00\x00\x08\x12\x72\x1d" // Null Offset
                                      "\x00\x03""bar" // topic 2 name
                                      "\x00\x00\x00\x02" // 2 partitions
                                          "\x00\x00\x00\x00" // partition 1 id
                                          "\x00\x00" // partition 1 err_code
                                          "\x00\x00\x00\x00\x07\x5b\xcd\x15" // Null Offset
                                          "\x00\x00\x00\x01" // partition 2 id
                                          "\x00\x06" // partition 1 err_code
                                          "\x00\x00\x00\x00\x00\x00\x00\x00" // Null Offset
                              ,92)
                        ),
    };

    for (auto& t : test_cases) {
        auto r = std::get<0>(t);
        auto expected = std::get<1>(t);

        PacketEncoder pe(256);

        pe.io(r);
        ASSERT_TRUE(pe.ok()) << pe.err_str();

        auto encoded = pe.get_as_slice(false);
        EXPECT_EQ(expected.size(), encoded.size());
        ASSERT_EQ(0, expected.compare(encoded))
            << "Expected: <" << expected.hex() << "> ("<< expected.size() << ")\n"
            << "Got:      <" << encoded.hex() << "> ("<< encoded.size() << ")";

        // Read it back
        proto::ProduceResponse r2;

        PacketDecoder pd(buffer_from_string(encoded.str()));

        pd.io(r2);

        EXPECT_TRUE(pd.ok()) << pd.err_str();

        EXPECT_EQ(r.topics.size(), r2.topics.size());

        int idx = 0;
        for (auto& topic : r2.topics) {
            auto expected_topic = r.topics[idx];
            EXPECT_EQ(expected_topic.name, topic.name);
            EXPECT_EQ(expected_topic.partitions.size(), topic.partitions.size());

            int jdx = 0;
            for (auto& partition : topic.partitions) {
                auto expected_partition = expected_topic.partitions[jdx];

                EXPECT_EQ(expected_partition.partition_id, partition.partition_id);
                EXPECT_EQ(expected_partition.err_code, partition.err_code);
                EXPECT_EQ(expected_partition.offset, partition.offset);

                ++jdx;
            }

            ++idx;
        }
    }
}