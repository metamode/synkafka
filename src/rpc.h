#pragma once

#include <memory>

#include <boost/asio.hpp>
#include <boost/asio/coroutine.hpp>



using boost::asio::ip::tcp;
using boost::system::error_code;

namespace synkafka
{

template<RequestType, ResponseType>
class RPC : boost::asio::coroutine
{
public:
	RPC(std::shared_ptr<tcp::socket> socket, std::shared_ptr<boost::asio::io_service::strand> strand);

	// Coroutine to handle continuing this request
	void operator()(error_code ec = error_code()
				   ,size_t length = 0
				   );

private:
	std::shared_ptr<tcp::socket>						socket_;
	std::shared_ptr<boost::asio::io_service::strand>	strand_;
	shared_buffer_t										buffer_;
};




class RPC
{
public:

	void exec(error_code ec = error_code()
			 ,size_t length = 0
			 )
	{
		if (!ec) {
			reenter (this)
			{
				// encode request
				// yield async_write(.., this);

				// now we handled our own write success/fail
				// we can just yield to the pipeline to coordinate our
				// reads with any others in flight
				// yield pipeline_.sent(this);

				// Now we are back into reading state as called by pipeline processor

				// read response

				// when done/failed, resolve promise
				// pipeline_.done(this);
			}
		}
	}
};

class RPCPipeline
{
public:
	void send(RPC&& rpc)
	{
		// Assign seq id
		// set rpc processor to be bound call to process()
		// push to queue
		// async_write -> handle_write(rpc)
	}

	void handle_write(RPC& rpc, ec, n)
	{
		// if fail clean up
		// otherwise 
		if (read_q_head_.seq == rpc.seq) {
			// We are at head of read q
		}
	}

	void sent(RPC& rpc)
	{
		rpc.sent = true;
		if (rpc.seq == q_.front().seq)
		{
			// Waiting RPC is at front of queue
			// continue it's read processing
			rpc();
		}
	}

	void done(RPC& rpc)
	{
		// called by rpc onces it's completed handling
		// by construction it can only be called by the head of the queue
		// so pop that off
		// q.pop();
		// Then if there are any more RPCs in queue, get the next one and resume it.
		// take care not to call coroutine while write is still pending
		auto rpc = q_.front();
		if (rpc.sent) {
			// Send completed, finish reading
			rpc();
		}
	}


};

}