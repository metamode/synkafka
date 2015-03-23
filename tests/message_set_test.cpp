#include "gtest/gtest.h"

#include "message_set.h"

using namespace synkafka;


TEST(MessageSet, Construction)
{
    MessageSet ms;

    auto ec = ms.push("test message", "");

    size_t expected_size = sizeof(int64_t) // offset
                         + sizeof(int32_t) // message length
                         + sizeof(int32_t) // message crc
                         + sizeof(int8_t) // magic
                         + sizeof(int8_t) // attributes
                         + sizeof(int32_t) // key length prefix (no key bytes)
                         + sizeof(int32_t) // value length prefix (no key bytes)
                         + strlen("test message") // value bytes
                         ;

    EXPECT_FALSE(ec);

    EXPECT_EQ(expected_size, ms.get_encoded_size());

    // Test message set refuses messages with larger than max size
    ms.set_max_message_size(50); // Message above takes up 38 bytes so this is not enough for another one.

    ec = ms.push("test message", "");

    EXPECT_TRUE((bool)ec);
    EXPECT_EQ(synkafka_error::message_set_full, ec);
}