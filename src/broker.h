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
#include "log.h"

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

	template<typename RequestType, typename ResponseType>
	std::error_code sync_call(RequestType& request, ResponseType& resp, int32_t timeout_ms)
	{
		auto enc = std::make_shared<PacketEncoder>(512);
		enc->io(request);

		if (!enc->ok()) {
			log->error("Failed to encode request: ") << enc->err_str();
			return make_error_code(synkafka_error::encoding_error);
		}

	    auto decoder_future = call(RequestType::api_key, std::move(enc));

	    auto status = decoder_future.wait_for(std::chrono::milliseconds(timeout_ms));

	    if (status != std::future_status::ready) {
			return make_error_code(synkafka_error::network_timeout);
	    } else {
		    // OK we got a result, decode it
		    try 
		    {
		    	auto decoder = decoder_future.get();
			    decoder.io(resp);

				if (!decoder.ok()) {
					log->error("Failed to decoder packet: ") << decoder.err_str();
					return make_error_code(synkafka_error::decoding_error);
				}
		    }
		    catch (const std::error_code& errc)
		    {
		    	log->error("Failed sync_call with error_code: ") << errc.message();
		    	return errc;
		    }
		    catch (const std::future_error& e)
		    {
		    	log->error("Failed sync_call with future_error: ") << e.code().message();
		    	return e.code();
		    }
		    catch (const std::exception& e)
		    {
		    	log->error("Failed sync_call with exception: ") << e.what();
		    	return make_error_code(synkafka_error::unknown);
		    }
		    catch (...)
		    {
		    	log->error("Failed sync_call with unknown exception");
		    	return make_error_code(synkafka_error::unknown);
		    }
	    }

	    return make_error_code(synkafka_error::no_error);
	}


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