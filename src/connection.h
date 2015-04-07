#pragma once

#include <ostream>
#include <condition_variable>
#include <memory>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/coroutine.hpp>


using boost::asio::ip::tcp;
using boost::system::error_code;

namespace synkafka
{

// Connection manages process of (blocking, multithreaded) connect of a single tcp socket
// with timeout
class Connection : boost::asio::coroutine
{
public:
	Connection(boost::asio::io_service& io_service, const std::string& host, int32_t port);

	// Set the timeout for connection, default is 1 second (1000 ms)
	void set_timeout(int32_t milliseconds) { pimpl_->timeout_ms_ = milliseconds; }

	// Safe to call from multiple threads
	// Call will block until socket is connected, fails,
	// or the timeout is met.
	error_code connect();

	// Entry point for connection coroutine - not to be called externally
	// although must be public for asio to hook into it
	typedef void result_type; // Allows boost::bind to bind arguments to functor calls...
	void operator()(error_code ec = error_code(), tcp::resolver::iterator endpoint_iterator = tcp::resolver::iterator());

	// Forcefully close the socket
	// this is thread safe, any outstanding asio requests on the socket will
	// return with a failure.
	// If socket is already closed, this will return the error condition that
	// caused it to close originally. Otherwise it will return the error condition
	// supplied.
	error_code close(error_code ec = error_code());

	bool is_connected() const { return pimpl_->state_ == STATE_CONNECTED; };
	bool is_closed() const { return pimpl_->state_ == STATE_CLOSED; };

	// Access the socket for reading/writing, this will simply proxy through to composed async read/write
	// on underlying socket. Provided to avoid leaking actual socket object from abstraction.
	// handler must be wrapped on a strand or called from a single asio thread to avoid races.
	template<typename MutableBufferSequence, typename ReadHandler>
	void async_read(const MutableBufferSequence& buffers, ReadHandler handler)
	{
		boost::asio::async_read(pimpl_->socket_, buffers, handler);
	}

	template<typename ConstBufferSequence, typename WriteHandler>
	void async_write(const ConstBufferSequence& buffers, WriteHandler handler)
	{
		boost::asio::async_write(pimpl_->socket_, buffers, handler);
	}

	// Debugging
	friend std::ostream& operator<<(std::ostream& os, const Connection& conn);


private:

	typedef enum { STATE_INIT, STATE_CONNECTING, STATE_CONNECTED, STATE_CLOSED } ConnectionState;

	// Pimpl pattern since whole Functor must be trivially copyable to be a valid asio handler
	// this means we just keep a shared pointer to entire state and only pay price of one shared_ptr
	// copy each time.
	struct impl
	{
		impl(boost::asio::io_service& io_service, const std::string& host, int32_t port);

		error_code close(error_code ec = error_code());

		tcp::resolver::query 			dns_query_;
		tcp::socket 					socket_;
		tcp::resolver  					resolver_;
		int32_t 						timeout_ms_;
		std::mutex 						mu_;
		std::condition_variable			cv_;
		ConnectionState					state_;
		error_code 						ec_;
	};

	std::shared_ptr<impl> pimpl_;
};

}