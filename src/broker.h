#pragma once

#include <condition_variable>
#include <future>
#include <list>
#include <memory>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "protocol.h"

using boost::asio::ip::tcp;
using boost::system::error_code;

namespace synkafka {

/**
 * The main client interface for talking to a kafka broker
 */
class Broker : public boost::enable_shared_from_this<Broker>,
	private boost::noncopyable
{
public:
	// Attempts to connect on startup
	Broker(boost::asio::io_service& io_service, const std::string& host, int32_t port, const std::string& client_id);
	~Broker();

	std::future<PacketDecoder> call(int16_t api_key, std::shared_ptr<PacketEncoder> request_packet);


	// Could return false in 2 cases:
	//  1. We were connected but TCP connection died and was closed, or was closed externally (possibly due to taking to long for response)
	//  2. We were just instantiated and are still waiting for async resolve/connect to happen
	// If you need to distinguish the to cases, use is_closed() which will only return true in the second case.
	bool is_connected();
	bool is_closed();

	void close();


	// All these are executed on strand
	// Thery really shoudl be private but ASIO can't call private callbacks from its work queue
	struct InFlightRequest
	{
		proto::RequestHeader			header;
		std::shared_ptr<PacketEncoder> 	packet;
		bool 							sent;
		std::promise<PacketDecoder> 	response_promise;
	};

	std::error_code wait_for_connect(int32_t milliseconds);
	void handle_resolve(const error_code& err, tcp::resolver::iterator endpoint_iterator);
	void handle_connect(const error_code& err, tcp::resolver::iterator endpoint_iterator);

	void push_request(std::shared_ptr<InFlightRequest> req);
	void write_next_request();

	void handle_write(const error_code& ec, size_t bytes_written);

	void response_handler_actor(const error_code& ec, size_t n = 0);

	const proto::Broker& get_config() const { return identity_; }

private:

	enum {
		StateInit,
		StateConnecting,
		StateConnected,
		StateClosed,
	};

	enum {
		RespHandlerStateIdle,
		RespHandlerStateReadHeader,
		RespHandlerStateReadResp,
	};

	std::string							client_id_;
	proto::Broker 						identity_;
	boost::asio::io_service::strand		strand_;
	tcp::resolver         				resolver_;
	tcp::socket         				socket_;
	uint32_t							next_correlation_id_;
	std::list<InFlightRequest>			in_flight_;
	int 								connection_state_;
	std::mutex							connection_mu_;
	std::condition_variable				connection_cv_;
	int 								response_handler_state_;
	shared_buffer_t 					header_buffer_;
	shared_buffer_t						response_buffer_;
};

}