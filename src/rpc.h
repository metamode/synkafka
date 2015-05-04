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
#include "log.h"

namespace synkafka
{

using boost::asio::ip::tcp;
using boost::system::error_code;

class RPC
{
public:
    RPC() = default;
    RPC(int16_t api_key, std::unique_ptr<PacketEncoder> encoder, slice client_id);

    void set_seq(int32_t seq);
    int32_t get_seq() const;
    int16_t get_api_key() const;
    PacketDecoder* get_decoder();

    const std::vector<boost::asio::const_buffer> encode_request();

    shared_buffer_t get_recv_buffer();
    std::future<PacketDecoder> get_future();

    void fail(std::error_code ec);

    template<typename ErrC>
    void fail(ErrC errc)
    {
        fail(make_error_code(errc));
    }

    void resolve();

private:
    int32_t                         seq_;
    int16_t                         api_key_;
    slice                           client_id_;
    std::unique_ptr<PacketEncoder>  header_encoder_;
    std::unique_ptr<PacketEncoder>  encoder_;
    shared_buffer_t                 response_buffer_;
    std::unique_ptr<PacketDecoder>  decoder_;
    std::promise<PacketDecoder>     response_promise_;
};

typedef std::function<void (std::unique_ptr<RPC>)> rpc_success_handler_t;

// Abstract Queue for seqentially performing async work on a connection
// Needed since we must have both senders and recievers synchronised.
// Uses boost::asio::coroutin stackless coroutine implementation but NOT
// in the normal way - we don't extend the coroutine class because we need
// to re-start the handling coroutine every time the queue is emptied and then
// a new request is pushed. boost::asio::coroutine is actually just a very simple class
// wrappin an int which holds co-routine state, if the current co-routine is not set
// or has terminated when we push we create a new one (i.e. the state) but handler
// stays in the class.
// Usage in the operator() implementation is as normal except that you must use
//      reenter (pimpl_->coro_)
// rather than
//   reenter (this)
class RPCQueue
{
public:
    RPCQueue(Connection conn, rpc_success_handler_t on_success);

    // This is the coroutine entry point
    virtual void operator()(error_code ec = error_code()
                           ,size_t length = 0
                           ) = 0;

    void push(std::unique_ptr<RPC> rpc);

protected:

    // All of these MUST be called on connection's strand
    void stranded_push(RPC* rpc);

    virtual const std::string& queue_type() const = 0;
    virtual bool should_increment_seq_on_push() const = 0;

    void fail_all(std::error_code ec);
    void fail_all(error_code ec); // Boost error_code..
    RPC* next();
    std::unique_ptr<RPC> pop();

    struct Impl
    {
        Impl(Connection conn, rpc_success_handler_t on_success);

        Connection                          conn_;
        std::deque<std::unique_ptr<RPC>>    q_;
        int32_t                             next_seq_; // only really needed for send queue but..
        rpc_success_handler_t               on_success_;
        boost::asio::coroutine              coro_;
    };

    std::shared_ptr<Impl> pimpl_;
};

class RPCSendQueue : public RPCQueue
{
public:
    RPCSendQueue(Connection conn, rpc_success_handler_t on_success)
        : RPCQueue(std::move(conn), on_success)
    {}

    virtual void operator()(error_code ec = error_code()
                           ,size_t length = 0
                           ) override;

    virtual const std::string& queue_type() const override
    {
        static const std::string t = "SendQueue";
        return t;
    }

protected:
    virtual bool should_increment_seq_on_push() const { return true; }
};

class RPCRecvQueue : public RPCQueue
{
public:
    RPCRecvQueue(Connection conn, rpc_success_handler_t on_success)
        : RPCQueue(std::move(conn), on_success)
    {}

    virtual void operator()(error_code ec = error_code()
                           ,size_t length = 0
                           ) override;

    virtual const std::string& queue_type() const override
    {
        static const std::string t = "RecvQueue";
        return t;
    }

    virtual bool should_increment_seq_on_push() const { return false; }
};

}