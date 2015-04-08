#include <stdexcept>

#include <boost/bind.hpp>

#include "protocol.h"
#include "rpc.h"

using boost::asio::ip::tcp;
using boost::system::error_code;

namespace synkafka
{


RPC::RPC(int16_t api_key, std::shared_ptr<PacketEncoder> encoder, const std::string& client_id)
	:header_({api_key, KafkaApiVersion, 0, client_id})
	,encoder_(std::move(encoder))
	,response_buffer_(new buffer_t(128))
	,response_promise_()
{}

void RPC::set_seq(int32_t seq)
{
	header_.correlation_id = seq;
}

int32_t RPC::get_seq() const
{
	return header_.correlation_id;
}

const std::vector<boost::asio::const_buffer> RPC::encode_request()
{
	// We encode header as one buffer and push a sequence of header and request body
	// which was already encoded in calling thread. Buffer sequence allows us to do that
	// without copying again, but we need to include full length in the header buffer prefix.
	PacketEncoder header_encoder(64);
	header_encoder.io(header_);

	if (!header_encoder.ok()) {
		fail(synkafka_error::encoding_error);
		return {};
	}

	auto request_buffer = encoder_->get_as_slice(false);
	auto header_buffer = header_encoder.get_as_buffer_sequence_head(request_buffer.size());

	return std::vector<boost::asio::const_buffer>{{header_buffer.data(), header_buffer.size()}
												 ,{request_buffer.data(), request_buffer.size()}
												 };
}

shared_buffer_t RPC::get_recv_buffer()
{
	return response_buffer_;
}
	
std::future<PacketDecoder> RPC::get_future()
{
	return response_promise_.get_future();
}

void RPC::fail(std::error_code ec)
{
	response_promise_.set_exception(std::make_exception_ptr(ec));
}

void RPC::resolve(PacketDecoder&& decoder)
{
	response_promise_.set_value(std::move(decoder));
}

void RPC::swap(RPC& other)
{
	std::swap(header_, other.header_);
	std::swap(encoder_, other.encoder_);
	std::swap(response_buffer_, other.response_buffer_);
	std::swap(response_promise_, other.response_promise_);
}


RPCQueue::Impl::Impl(std::shared_ptr<Connection> conn, rpc_success_handler_t on_success)
	:conn_(std::move(conn))
	,q_()
	,next_seq_(0)
	,on_success_()
{}

RPCQueue::RPCQueue(std::shared_ptr<Connection> conn, rpc_success_handler_t on_success)
	:pimpl_(new Impl(std::move(conn), on_success))
{}

void RPCQueue::push(RPC&& rpc)
{
	pimpl_->conn_->get_strand().post(boost::bind(&RPCQueue::stranded_push, this, std::ref(rpc)));
}

void RPCQueue::stranded_push(RPC& rpc)
{	
	if (should_increment_seq_on_push()) {
		rpc.set_seq(pimpl_->next_seq_++);
	}

	pimpl_->q_.push_back(std::move(rpc));

	if (pimpl_->q_.size() == 1) {
		// Was empty before, start coroutine processing
		// the new queue.
		(*this)();
	}
}

void RPCQueue::fail_all(error_code ec)
{
	// Close socket so whole broker connection is torn down and re-established
	pimpl_->conn_->close(ec);

	// This will also call close but will be a no-op this way we get to propagate boost::system errors
	fail_all(std::make_error_code(static_cast<std::errc>(ec.value())));
}

void RPCQueue::fail_all(std::error_code ec)
{
	while (auto rpc = next()) {
		rpc->fail(ec);
		pop();
	}
	// Close (in case it isn't already closed due to boost error)
	pimpl_->conn_->close();
}

RPC* RPCQueue::next()
{
	if (!pimpl_->q_.empty()) {
		return &(pimpl_->q_.front());
	}
	return nullptr;
}

void RPCQueue::pop()
{
	if (!pimpl_->q_.empty()) {
		pimpl_->q_.pop_front();
	}
}

// Enable the pseudo-keywords reenter, yield and fork.
#include <boost/asio/yield.hpp>

void RPCSendQueue::operator()(error_code ec, size_t length)
{
	if (ec) {
		fail_all(ec);
		return;
	}

	RPC* rpc = next();

	reenter (this)
	{
		while (rpc) {
			yield pimpl_->conn_->async_write(rpc->encode_request(), *this);

			// Write was successful, handle success on the RPC and then
			// pop it from queue.
			{
				RPC complete_rpc;

				// Should be nothrow swap
				rpc->swap(complete_rpc);

				// Now we can pop the swapped husk from queue safely
				pop();

				if (pimpl_->on_success_) {
					pimpl_->on_success_(std::move(complete_rpc));
				}
			}
		}
	}
}

void RPCRecvQueue::operator()(error_code ec, size_t length)
{
	if (ec) {
		fail_all(ec);
		return;
	}

	RPC* rpc = next();
	auto buffer = rpc->get_recv_buffer();
	PacketDecoder pd(buffer);
	int32_t response_len;
	size_t offset;

	reenter (this)
	{
		while (rpc) {
			yield pimpl_->conn_->async_read(boost::asio::buffer(&(*buffer)[0], buffer->size()), *this);

			// Decode response header
			pd.io(response_len);

			if (!pd.ok()) {
				fail_all(make_error_code(synkafka_error::decoding_error));
				return;
			}

			// See if we have any more to read

			if (response_len > (buffer->size() - sizeof(response_len))) {
				// There are more bytes to read that didn't fit in the buffer.
				// We picked a largisih size that we expect to handle most buffer sizes
				// we'll need in practice (producer/meta responses are smallish) so this shouldn't
				// be necessary all the time, but in this case pay the realloc cost once.
				offset = buffer->size();

				buffer->resize(sizeof(response_len) + response_len);

				// Now read the rest of the bytes into the rest of the buffer
				yield pimpl_->conn_->async_read(boost::asio::buffer(&(*buffer)[0] + offset, buffer->size() - offset), *this);

				// Now have the whole thing, confusingly we need to re-parse response length just to reset cursor to the correct place
				// since PacketDecoder was recreated between calls. Could store the PacketDecoder in RPC I guess, Hmm.
				pd.io(response_len);
			}

			// We should now have the WHOLE response in buffer.

			// Read the correlation id
			{
				proto::ResponseHeader h;
				pd.io(h);

				if (!pd.ok()) {
					fail_all(make_error_code(synkafka_error::decoding_error));
					return;
				}

				if (h.correlation_id != rpc->get_seq()) {
					fail_all(make_error_code(synkafka_error::decoding_error));
					return;
				}
			}

			// Read was successful, handle success on the RPC and then
			// pop it from queue.
			{
				RPC complete_rpc;

				// Should be nothrow swap
				rpc->swap(complete_rpc);

				// Now we can pop the swapped husk from queue safely
				pop();

				complete_rpc.resolve(std::move(pd));
			}
		}
	}
}

// Disable the pseudo-keywords reenter, yield and fork.
#include <boost/asio/unyield.hpp>

}