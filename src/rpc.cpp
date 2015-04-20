#include <stdexcept>

#include <cassert>
#include <boost/bind.hpp>

#include "protocol.h"
#include "rpc.h"

#define DBG_LOG() \
	log()->debug() << pimpl_->conn_ << queue_type() << " RPC[" << rpc->get_seq() << "] "

using boost::asio::ip::tcp;
using boost::system::error_code;

namespace synkafka
{

RPC::RPC(int16_t api_key, std::unique_ptr<PacketEncoder> encoder, const slice& client_id)
	:seq_(0)
	,api_key_(api_key)
	,client_id_(client_id)
	,header_encoder_(nullptr)
	,encoder_(std::move(encoder))
	,response_buffer_(new buffer_t(1024))
	,decoder_(new PacketDecoder(response_buffer_))
	,response_promise_()
{}

void RPC::set_seq(int32_t seq)
{
	seq_ = seq;
}

int32_t RPC::get_seq() const
{
	return seq_;
}

int16_t RPC::get_api_key() const
{
	return api_key_;
}

PacketDecoder* RPC::get_decoder()
{
	return decoder_.get();
}

const std::vector<boost::asio::const_buffer> RPC::encode_request()
{
	// We encode header as one buffer and push a sequence of header and request body
	// which was already encoded in calling thread. Buffer sequence allows us to do that
	// without copying again, but we need to include full length in the header buffer prefix.
	if (!header_encoder_) {
		header_encoder_.reset(new PacketEncoder(20));
		proto::RequestHeader header{api_key_, KafkaApiVersion, seq_, client_id_};
		header_encoder_->io(header);
	}

	if (!header_encoder_->ok()) {
		fail(synkafka_error::encoding_error);
		return {};
	}

	auto request_buffer = encoder_->get_as_slice(false);
	auto header_buffer = header_encoder_->get_as_buffer_sequence_head(request_buffer.size());

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

void RPC::resolve()
{
	response_promise_.set_value(std::move(*decoder_));
}

RPCQueue::Impl::Impl(Connection conn, rpc_success_handler_t on_success)
	:conn_(conn)
	,q_()
	,next_seq_(0)
	,on_success_(on_success)
{}

RPCQueue::RPCQueue(Connection conn, rpc_success_handler_t on_success)
	:pimpl_(new Impl(conn, on_success))
{}

void RPCQueue::push(std::unique_ptr<RPC> rpc)
{
	// We need to keep single ownership of the RPC, but ASIO will make a copy
	// of the argument as we pass it into the async on-strand method.
	// shared_ptr has it's own issues - we can't take ownership back and transfer it
	// into the queue later since the transient copies ASIO makes will still try to delete the
	// RPC after we've moved it into queue.
	// So we keep it as a unique_ptr to here, then release it so it's not destroyed, and finally
	// std::move it into the queue such that the queue will eventually destruct it when it's popped
	// Also note that we use dispatch() not post() so that if queue push is called form asio thread
	// there is no additional queuing delay. In practice this means that send queue pushes direct to recv queue
	// and if appropriate initiaties async read right away rather than waiting for another trip through ASIO's internal
	// queue.
	pimpl_->conn_.get_strand().dispatch(boost::bind(&RPCQueue::stranded_push
											   	   ,this
											   	   ,rpc.release()));
}

void RPCQueue::stranded_push(RPC* rpc)
{
	if (should_increment_seq_on_push()) {
		rpc->set_seq(pimpl_->next_seq_++);
	}

	DBG_LOG() << "Stranded Queue Push";

	// Transfer the pointed to RPC, we are giving the queue ownership of the pointed to RPC again
	pimpl_->q_.emplace_back(rpc);

	if (pimpl_->q_.size() == 1) {
		// Was empty before, start coroutine processing
		if (pimpl_->coro_.is_complete()) {
			pimpl_->coro_ = boost::asio::coroutine();
		}
		(*this)();
	}
}

void RPCQueue::fail_all(error_code ec)
{
	// Close socket so whole broker connection is torn down and re-established
	pimpl_->conn_.close(ec);

	// This will also call close but will be a no-op. This way we get to propagate boost::system errors
	fail_all(std::make_error_code(static_cast<std::errc>(ec.value())));
}

void RPCQueue::fail_all(std::error_code ec)
{
	// Make a local copy of the queue since as soon as we fail() RPC the calling
	// thread might destruct the broker from under us which will invalidate pimpl_'s memory
	// so do all the state mutations we need first, and only start failing things after that.
	std::list<std::unique_ptr<RPC>> local_q;

	std::swap(pimpl_->q_, local_q);

	// Close (in case it isn't already closed due to boost error)
	pimpl_->conn_.close();

	// Now fail all of the RPCs. Not that failing first one might
	// cause calling thread to destruct us, before we handle further ones,
	// but that's OK since we kept a local copy of the RPCs and can continue
	// to loop even if this queue and it's impl are destructed around our ears.
	for (auto& rpc : local_q) {
		rpc->fail(ec);
	}
}

RPC* RPCQueue::next()
{
	if (!pimpl_->q_.empty()) {
		return pimpl_->q_.front().get();
	}
	return nullptr;
}

std::unique_ptr<RPC> RPCQueue::pop()
{
	if (!pimpl_->q_.empty()) {
		auto rpc = std::move(pimpl_->q_.front());
		pimpl_->q_.pop_front();
		return rpc;
	}
	return std::unique_ptr<RPC>(nullptr);
}

// Enable the pseudo-keywords reenter, yield and fork.
#include <boost/asio/yield.hpp>

void RPCSendQueue::operator()(error_code ec, size_t length)
{
	RPC* rpc = next();

	if (!ec && !pimpl_->conn_.is_connected()) {
		// Can't easily fall through to below code since
		// we have different error_code types from boost and std mismatched here
		DBG_LOG() << "no connection, failing all";
		fail_all(make_error_code(synkafka_error::network_fail));
		return;
	}

	if (ec) {
		DBG_LOG() << "failing all";
		fail_all(ec);
		return;
	}

	reenter (pimpl_->coro_)
	{
		while (rpc) {
			DBG_LOG() << "starting send, api_key: " << rpc->get_api_key();
			yield pimpl_->conn_.async_write(rpc->encode_request(), *this);

			// Write was successful, handle success on the RPC and then
			// pop it from queue.
			{
				DBG_LOG() << "write complete, length: " << length;
				auto complete_rpc = pop();

				if (pimpl_->on_success_) {
					pimpl_->on_success_(std::move(complete_rpc));
					DBG_LOG() << "success handler run, popped rpc, queue length now: " << pimpl_->q_.size();
				}
			}

			rpc = next();
		}
	}
}

void RPCRecvQueue::operator()(error_code ec, size_t length)
{
	RPC* rpc = next();

	if (ec) {
		DBG_LOG() << "failing all";
		fail_all(ec);
		return;
	}

	shared_buffer_t buffer = rpc->get_recv_buffer();
	PacketDecoder* pd = rpc->get_decoder();
	int32_t response_len = 0;

	reenter (pimpl_->coro_)
	{
		while (rpc) {
			DBG_LOG() << "starting recv, api_key: " << rpc->get_api_key();

			// First up we need to read the response length
			assert(buffer->size() > sizeof(response_len));
			yield pimpl_->conn_.async_read(boost::asio::buffer(&(*buffer)[0], sizeof(response_len)), *this);

			// Now we have the response length
			pd->set_readable_length(sizeof(response_len));
			pd->io(response_len);
			if (!pd->ok()) {
				fail_all(make_error_code(synkafka_error::decoding_error));
				return;
			}

			DBG_LOG() << "recvd response length: " << response_len
				<< " (in " << length << " bytes)";

			// Read that many more bytes
			if (response_len > 0) {
				// See if our buffer is big enough for all of them (we need size of the length prefix + the length)
				if (buffer->size() < (sizeof(response_len) + response_len)) {
					buffer->resize(sizeof(response_len) + response_len);
				}

				// Now read the rest of the response
				// Start from after the length prefix
				yield pimpl_->conn_.async_read(boost::asio::buffer(&(*buffer)[0] + sizeof(response_len)
																  ,response_len
																  )
											  ,*this
											  );

				// Note that we re-entered after yield so response_len is initialised to 0 again
				// but we are guaranteed that async_read either read the whole length we asked for or
				// failed with an error which we would have caught above. So we can reset response_len from
				// the number of bytes that were actually read.
				// Tis' but a minor stain on the illusion of synchronous programming from this coroutine ;).
				response_len = length;

				// Read response header
				pd->set_readable_length(sizeof(response_len) + response_len);

				proto::ResponseHeader h;

				pd->io(h);

				if (!pd->ok()) {
					fail_all(make_error_code(synkafka_error::decoding_error));
					return;
				}

				DBG_LOG() << "recvd response, length: " << response_len
					<< ", correlation_id: " << h.correlation_id;

				if (h.correlation_id != rpc->get_seq()) {
					fail_all(make_error_code(synkafka_error::decoding_error));
					return;
				}
			}

			// Read was successful, handle success on the RPC and then
			// pop it from queue.
			pop()->resolve();

			DBG_LOG() << "resolved and popped rpc, queue length now: " << pimpl_->q_.size();

			rpc = next();

			if (rpc) {
				// Reset loop variables otherwise we'll still be looking at
				// previous rpc's buffers/decoder
				buffer = rpc->get_recv_buffer();
				pd = rpc->get_decoder();
				response_len = 0;
			}
		}
	}
}

// Disable the pseudo-keywords reenter, yield and fork.
#include <boost/asio/unyield.hpp>

}