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
				// yield pipeline_.waiting_on_read(this);

				// Now we are back into reading state as called by pipeline processor

				// read response

				// when done, failed, resolve promise
				// set complete_ = true
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
		// Start exec - this will do all the writing, up to the point the
		// RPC needs to be read from wire
		// rpc.exec()
	}

	void waiting_on_read(RPC& rpc)
	{
		if (rpc.seq == q_.front().seq)
		{
			// Waiting RPC is at front of queue
			// initiate read processing
			process();
		}
	}

	void process(error_code ec = error_code(), size_t length = 0)
	{
		// rpc = q_.front();
		// rpc.exec(ec, length);
		// if (rpc.complete()) {
		//		q_.pop()
		// 		if (!q_.empty()) q_.front().exec();
		// }
	}


};

}