#pragma once

#include <functional>
#include <future>
#include <memory>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/coroutine.hpp>

#include "buffer.h"
#include "connection.h"
#include "constants.h"
#include "packet.h"

using boost::asio::ip::tcp;
using boost::system::error_code;

namespace synkafka
{

class RPC
{
public:
	RPC() = default;
	RPC(int16_t api_key, std::shared_ptr<PacketEncoder> encoder, const std::string& client_id);

	void set_seq(int32_t seq);
	int32_t get_seq() const;

	const std::vector<boost::asio::const_buffer> encode_request();

	shared_buffer_t get_recv_buffer();
	std::future<PacketDecoder> get_future();

	void fail(std::error_code ec);

	template<typename ErrC>
	void fail(ErrC errc)
	{
		fail(make_error_code(errc));
	}

	void resolve(PacketDecoder&& decoder);

	void swap(RPC& other);

private:
	proto::RequestHeader			header_;
	std::shared_ptr<PacketEncoder>	encoder_;
	shared_buffer_t 				response_buffer_;
	std::promise<PacketDecoder> 	response_promise_;
};


typedef std::function<void (RPC&&)> rpc_success_handler_t;

class RPCQueue : protected boost::asio::coroutine
{
public:
	RPCQueue(std::shared_ptr<Connection> conn, rpc_success_handler_t on_success);

	virtual void operator()(error_code ec = error_code()
				   		   ,size_t length = 0
				   		   ) = 0;

	void push(RPC&& rpc);

protected:

	// All of these MUST be called on connection's strand
	void stranded_push(RPC& rpc);

	virtual bool should_increment_seq_on_push() const = 0;

	void fail_all(std::error_code ec);
	void fail_all(error_code ec); // Boost error_code..
	RPC* next();
	void pop();

	struct Impl
	{
		Impl(std::shared_ptr<Connection> conn, rpc_success_handler_t on_success);

		std::shared_ptr<Connection>			conn_;
		std::list<RPC> 						q_;
		int32_t        						next_seq_; // only really needed for send queue but..
		rpc_success_handler_t				on_success_;
	};

	std::shared_ptr<Impl> pimpl_;	
};

class RPCSendQueue : public RPCQueue
{
public:
	RPCSendQueue(std::shared_ptr<Connection> conn, rpc_success_handler_t on_success)
		: RPCQueue(std::move(conn), on_success)
	{}

	virtual void operator()(error_code ec = error_code()
				   ,size_t length = 0
				   ) override;
protected:
	virtual bool should_increment_seq_on_push() const { return true; }
};

class RPCRecvQueue : public RPCQueue
{
public:
	RPCRecvQueue(std::shared_ptr<Connection> conn, rpc_success_handler_t on_success)
		: RPCQueue(std::move(conn), on_success)
	{}

	virtual void operator()(error_code ec = error_code()
				   ,size_t length = 0
				   ) override;

	virtual bool should_increment_seq_on_push() const { return false; }
};

}