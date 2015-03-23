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

using namespace synkafka;

void run_asio(boost::asio::io_service& io_service)
{
	try
	{
    	io_service.run();
		std::cout << "ASIO THREAD EXITING" << std::endl;
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

TEST(Broker, BasicConnection)
{
    boost::asio::io_service io_service;
    std::auto_ptr<boost::asio::io_service::work> work(
    	new boost::asio::io_service::work(io_service)); // Keep io service running till we are done

    std::thread ios_thread(run_asio, std::ref(io_service));

    boost::shared_ptr<Broker> b(new Broker(io_service, "192.168.59.103", 49158, "test"));
    //boost::shared_ptr<Broker> b(new Broker(io_service, "localhost", 9000, "test"));

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

	    EXPECT_EQ(1, resp.brokers.size());
	    EXPECT_EQ("192.168.59.103", resp.brokers[0].host);
	    EXPECT_EQ(49158, resp.brokers[0].port);
	    EXPECT_EQ(49158, resp.brokers[0].node_id); // This is how our test setup assigns IDs

	    // Expect (at least) one topic called test with 8 partitions...
	    EXPECT_EQ(1, resp.topics.size());
	    EXPECT_EQ("test", resp.topics[0].name);
	    EXPECT_EQ(8, resp.topics[0].partitions.size());
    }

    work.reset(); // Exit asio thread
    io_service.stop();
    ios_thread.join();
}