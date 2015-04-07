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

#include "connection.h"
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

	/*template<typename RequestType, typename ResponseType>
	std::future<ResponseType> call(RequestType&& request)
	{
		// make shared request ptr

		// Get future from promise

		// dispatch to send() on strand with req ptr as first param

		// return future
	}

	template<typename RequestType, typename ResponseType>
	struct RPCRequest
	{
		int32_t 					seq;
		RequestType 				req;
		std::promise<ResponseType> 	resp;
		buffer_t 					read_buffer; // start at some size that will be enough for mos responses maybe 1k?
		size_t 						response_len;
		size_t						recvd_len;
	};

	template<typename RequestType, typename ResponseType>
	void send(std::shared_ptr<RPCRequest<RequestType, ResponseType>>req)
	{
		// On strand now, assign next seqence number to request
		// And encode the packet
		// call async write if OK -> handle_send
	}	

	template<typename RequestType, typename ResponseType>
	void handle_send(std::shared_ptr<RPCRequest<RequestType, ResponseType>> req, const error_code& ec, size_t n)
	{
		// if error, abort promise
		// if OK async read -> handle_response
	}

	template<typename RequestType, typename ResponseType>
	void handle_response(std::shared_ptr<RPCRequest<RequestType, ResponseType>> req, const error_code& ec, size_t n)
	{
		// if error, abort promise
		// if req->response_len == 0 this is intial read
			// try decode header or fail
		    // set req->response_len = decoded_len, req->recvd_len = n
		    // if n = decoded response length + sizeof(int32_t), we have whole thing, decode it 
		    // else figure out how much more there is to read, resize buffer to right size (+ lenth prefix)
		    //   and read async -> handle_response
		// else, decode full buffer into ResponseType struct
		// fullfil promise or fail it 
	}*/


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

	void set_connect_timeout(int32_t milliseconds) { conn_.set_timeout(milliseconds); }

	// Blocking, thread-safe call.
	// Must be called OUTSIDE asio thread
	std::error_code connect();

	bool is_connected() const { return conn_.is_connected(); }
	bool is_closed() const { return conn_.is_closed(); }

	void push_request(std::shared_ptr<InFlightRequest> req);
	void write_next_request();

	void handle_write(const error_code& ec, size_t bytes_written);

	void response_handler_actor(const error_code& ec, size_t n = 0);

	const proto::Broker& get_config() const { return identity_; }

private:

	enum {
		RespHandlerStateIdle,
		RespHandlerStateReadHeader,
		RespHandlerStateReadResp,
	};

	std::string							client_id_;
	proto::Broker 						identity_;
	boost::asio::io_service::strand		strand_;
	Connection 							conn_;
	uint32_t							next_correlation_id_;
	std::list<InFlightRequest>			in_flight_;
	int 								response_handler_state_;
	shared_buffer_t 					header_buffer_;
	shared_buffer_t						response_buffer_;
};

}