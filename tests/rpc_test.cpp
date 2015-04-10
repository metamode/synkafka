#include "gtest/gtest.h"

#include <boost/asio.hpp>

#include "packet.h"
#include "protocol.h"
#include "rpc.h"
#include "slice.h"

using namespace synkafka;

slice buf_to_slice(const boost::asio::const_buffer& b)
{
    return slice(boost::asio::buffer_cast<const unsigned char*>(b)
                ,boost::asio::buffer_size(b)
                );
}

TEST(RPC, Encoding)
{

    std::future<int> f;

    proto::TopicMetadataRequest rq;
    std::unique_ptr<PacketEncoder> enc(new PacketEncoder(10));
    enc->io(rq);

    ASSERT_TRUE(enc->ok());

    RPC rpc(ApiKey::MetadataRequest, std::move(enc), "tester");

    rpc.set_seq(1234);

    auto buffers = rpc.encode_request();

    ASSERT_EQ(2, buffers.size());

    slice header_expected("\x00\x00\x00\x14" // i32 Length prefix of whole packet (16 header + 4 rpc)
                          "\x00\x03" // MetadataRequest api key
                          "\x00\x00" // Version 0
                          "\x00\x00\x04\xd2" // 1234 (correlation_id)
                          "\x00\x06" // 6 bytes of string
                          "tester" // string client id
                          ,20
                          );
    slice request_expected("\x00\x00\x00\x00" // length prefix for the array of topics - empty
                          ,4
                          );

    auto enc_header = buf_to_slice(buffers[0]);
    EXPECT_EQ(header_expected, enc_header)
        << "Expected: <" << header_expected.hex() << "> ("<< header_expected.size() << ")\n"
        << "Got:      <" << enc_header.hex() << "> ("<< enc_header.size() << ")";

    auto enc_req = buf_to_slice(buffers[1]);

    EXPECT_EQ(request_expected, enc_req)
        << "Expected: <" << request_expected.hex() << "> ("<< request_expected.size() << ")\n"
        << "Got:      <" << enc_req.hex() << "> ("<< enc_req.size() << ")";
}