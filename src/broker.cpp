#include <cassert>
#include <future>
#include <string>
#include <stdexcept>

#include <boost/bind.hpp>

#include "broker.h"
#include "log.h"

using boost::asio::ip::tcp;
using boost::system::error_code;

namespace synkafka {

std::error_code std_from_boost_ec(error_code ec)
{
	return std::make_error_code(static_cast<std::errc>(ec.value()));
}

Broker::Broker(boost::asio::io_service& io_service, const std::string& host, int32_t port, const std::string& client_id)
	:client_id_(client_id)
	,identity_({0, host, port})
	,strand_(io_service)
	,conn_(io_service, host, port)
	,next_correlation_id_(1)
	,in_flight_()
	,response_handler_state_(RespHandlerStateIdle)
	,header_buffer_(new buffer_t(sizeof(int32_t) + sizeof(int32_t))) // Response header is just a single length prefix plus the correlation id currently
	,response_buffer_()
{
	init_log();
}

Broker::~Broker()
{
	close();
}

void Broker::close()
{	
	conn_.close();
}

std::future<PacketDecoder> Broker::call(int16_t api_key, std::shared_ptr<PacketEncoder> request_packet)
{
	// Correlation ID is choosen when we actual write to ensure correct sequencing
	// not sure it matters provided the writes are same order as queuing but it's simpler.
	std::shared_ptr<InFlightRequest> r(new InFlightRequest({proto::RequestHeader{api_key, KafkaApiVersion, 0, client_id_}
				     			 						   ,std::move(request_packet)
				     			 						   ,false
					 			 						   ,std::promise<PacketDecoder>()
					 			 						   }));

	auto f = r->response_promise.get_future();

	strand_.post(boost::bind(&Broker::push_request, shared_from_this(), std::move(r)));

	return f;
}

std::error_code Broker::connect()
{
	return std_from_boost_ec(conn_.connect());
}

void Broker::push_request(std::shared_ptr<InFlightRequest> req)
{
	log->debug("push_request");

	if (conn_.is_closed()) {
		// If we already closed connection, don't even bother queueing it
		log->debug("push_request failing request: already closed connection");
		req->response_promise.set_exception(std::make_exception_ptr(make_error_code(synkafka_error::network_fail)));
		return;
	}

	// Pick next ID
	req->header.correlation_id = next_correlation_id_++;

	// Insert into our "queue"
	in_flight_.push_back(std::move(*req));

	// If we are connected, do the async write
	// if not, then write_next_request will be called on connection success
	if (conn_.is_connected()) {
		write_next_request();
	}
}

void Broker::write_next_request()
{
	log->debug("write_next_request");

	if (in_flight_.empty()) {
		return;
	}

	auto req = in_flight_.begin();

	if (req->sent) {
		// TODO: figure out why I added this. Is it even possible?
		return;
	}

	// Write it to socket, we have separate bufers for header which we only just got the correlationid for
	// and actual request body which was passed in from external thread so write them as one with a const
	// buffer sequence
	std::vector<slice> buffers;
	buffers.push_back(req->packet->get_as_slice(false));

	// Now push on request header to buffer sequence such that it will write packet length for whole sequence
	// without copying the whole encoded message...
	PacketEncoder header_encoder(64);
	header_encoder.io(req->header);
	if (!header_encoder.ok()) {
		// TODO error out the request and pop it - connection can stay open
		return;
	}

	auto request_buffer = req->packet->get_as_slice(false);
	auto header_buffer = header_encoder.get_as_buffer_sequence_head(request_buffer.size());

	std::vector<boost::asio::const_buffer> sequence({});

	conn_.async_write(std::vector<boost::asio::const_buffer>{boost::asio::buffer(header_buffer.data(), header_buffer.size())
									       					,boost::asio::buffer(request_buffer.data(), request_buffer.size())
									       					}
					  ,strand_.wrap(boost::bind(&Broker::handle_write
					  						   ,shared_from_this()
			  		                 		   ,boost::asio::placeholders::error
			  		                 		   ,boost::asio::placeholders::bytes_transferred
			  		                 		   )
					  			   )
					  );
}

void Broker::handle_write(const error_code& ec, size_t bytes_written)
{
	if(in_flight_.empty()) {
		log->emerg("Write completed but in_flight queue is empty");
		return;
	}

	auto req = in_flight_.begin();
	if (ec) {
		// Fail the request
		log->debug("handle_write failing request: ") << (ec ? ec.message() : "0") << " bytes_written: " << bytes_written;
		req->response_promise.set_exception(std::make_exception_ptr(boost::system::system_error(ec)));
		in_flight_.pop_front();
	} else {
		req->sent = true;

		if (response_handler_state_ == RespHandlerStateIdle) {
			log->debug("handle_write sent OK (") << bytes_written << " bytes written). Starting Response handler.";
			strand_.post(boost::bind(&Broker::response_handler_actor, shared_from_this(), error_code(), 0));
		} 
	}
}

void Broker::response_handler_actor(const error_code& ec, size_t n)
{
	if (!conn_.is_connected() || in_flight_.empty()) {
		return;
	}

	auto req = in_flight_.begin();

	// Any read error is treated as fatal since we rely on ordering to work correctly
	if (ec) {
		log->debug("response_handler_actor read error, failing request: ") << (ec ? ec.message() : "0") << " bytes_read: " << n;
		req->response_promise.set_exception(std::make_exception_ptr(boost::system::system_error(ec)));
		close();
		return;
	}

	switch (response_handler_state_)
	{
	case RespHandlerStateIdle:
		response_handler_state_ = RespHandlerStateReadHeader;

		log->debug("response_handler_actor Idle -> ReadHeader ");
		
		// Initiate new read (we know no other read is occuring since we only execute on strand)
		conn_.async_read(boost::asio::buffer(&header_buffer_->at(0), header_buffer_->size())
						,strand_.wrap(boost::bind(&Broker::response_handler_actor
					   				  			 ,shared_from_this()
								                 ,boost::asio::placeholders::error
								                 ,boost::asio::placeholders::bytes_transferred
								                 )
						  			 )
						);
		break;

	case RespHandlerStateReadHeader:
		{
			response_handler_state_ = RespHandlerStateReadResp;
		
			log->debug("response_handler_actor ReadHeader -> ReadResponse ec:") << (ec ? ec.message() : "0") << " bytes read: " << n;

			// We now have the response length prefix and the header

			PacketDecoder pd(header_buffer_);

			int32_t response_len;
			proto::ResponseHeader h;

			pd.io(response_len);
			pd.io(h);

			if (!pd.ok()) {
				log->debug("response_handler_actor packet decoding error failing request: ") << pd.err_str();
				req->response_promise.set_exception(std::make_exception_ptr(std::runtime_error(pd.err_str())));
				close();
				return;
			}

			// Check correlationid is actually correct
			if (req->header.correlation_id != h.correlation_id) {
				log->debug("response_handler_actor correlation_id mismatch expected: ") 
					<< req->header.correlation_id << " Got: " << h.correlation_id
					<< " response length is " << response_len << " read " << n;
				req->response_promise.set_exception(std::make_exception_ptr(std::runtime_error("Correlation ID mismatch")));
				close();
				return;
			}

			// response_len includes the 4 byte correlation ID we already read into header buffer...
			response_len -= sizeof(h.correlation_id);

			// Make a new buffer the right size
			response_buffer_.reset(new buffer_t(response_len));
		
			log->debug("response_handler_actor waiting for reponse read of ") << response_len << " bytes. Correlation ID matched: " << h.correlation_id;

			// Initiate new read (we know no other read is occuring since we only execute on strand)
			conn_.async_read(boost::asio::buffer(&response_buffer_->at(0), response_buffer_->size())
							,strand_.wrap(boost::bind(&Broker::response_handler_actor
						  						     ,shared_from_this()
							                         ,boost::asio::placeholders::error
							                         ,boost::asio::placeholders::bytes_transferred
							                         )
										 )
							);
		}
		break;

	case RespHandlerStateReadResp:
		{
			response_handler_state_ = RespHandlerStateIdle;
		
			log->debug("response_handler_actor ReadResp -> Idle ec:") << (ec ? ec.message() : "0") << " bytes read: " << n;

			// We know correlationids matched already, now check we read enough...
			if (n < response_buffer_->size()) {
				log->debug("response_handler_actor failing, not enough bytes read. wanted ") << response_buffer_->size() << " got " << n;
				req->response_promise.set_exception(std::make_exception_ptr(std::runtime_error("Not enough bytes read")));
				close();
				return;
			}

			// We now have the whole response
			PacketDecoder pd(response_buffer_);

			auto req = in_flight_.begin();

			// Satisfy promise
			req->response_promise.set_value(std::move(pd));

			// All done with request
			in_flight_.pop_front();

			// Any more in queue? And are they sent already?
			if (!in_flight_.empty()) {
				req = in_flight_.begin();
				if (req->sent) {
					// Move right back to reading next request
					response_handler_actor(ec, 0);
				} 
			}

		}
		break;
	}
}

}