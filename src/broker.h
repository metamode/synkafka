#pragma once

#include <future>
#include <memory>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "connection.h"
#include "protocol.h"
#include "log.h"
#include "rpc.h"

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

	std::future<PacketDecoder> call(int16_t api_key, std::unique_ptr<PacketEncoder> request_packet);

	template<typename RequestType, typename ResponseType>
	std::error_code sync_call(RequestType& request, ResponseType& resp, int32_t timeout_ms)
	{
		auto enc = std::unique_ptr<PacketEncoder>(new PacketEncoder(512));
		enc->io(request);

		if (!enc->ok()) {
			log()->error("Failed to encode request: ") << enc->err_str();
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
					log()->error("Failed to decode packet: ") << decoder.err_str();
					return make_error_code(synkafka_error::decoding_error);
				}
		    }
		    catch (const std::error_code& errc)
		    {
		    	log()->error("Failed sync_call with error_code: ") << errc.message();
		    	return errc;
		    }
		    catch (const std::future_error& e)
		    {
		    	log()->error("Failed sync_call with future_error: ") << e.code().message();
		    	return e.code();
		    }
		    catch (const std::exception& e)
		    {
		    	log()->error("Failed sync_call with exception: ") << e.what();
		    	return make_error_code(synkafka_error::unknown);
		    }
		    catch (...)
		    {
		    	log()->error("Failed sync_call with unknown exception");
		    	return make_error_code(synkafka_error::unknown);
		    }
	    }

	    return make_error_code(synkafka_error::no_error);
	}


	void close();

	void set_node_id(int32_t node_id) { identity_.node_id = node_id; }
	void set_connect_timeout(int32_t milliseconds) { conn_.set_timeout(milliseconds); }

	// Blocking, thread-safe call.
	// Must be called OUTSIDE asio thread
	std::error_code connect();

	bool is_connected() const { return conn_.is_connected(); }
	bool is_closed() const { return conn_.is_closed(); }

	const proto::Broker& get_config() const { return identity_; }

private:

	std::string							client_id_;
	proto::Broker 						identity_;
	Connection 							conn_;
	RPCSendQueue						send_q_;
	RPCRecvQueue						recv_q_;
};

}