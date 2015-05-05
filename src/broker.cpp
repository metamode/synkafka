#include <cassert>
#include <future>
#include <string>
#include <stdexcept>

#include <boost/bind.hpp>

#include "broker.h"
#include "log.h"

namespace synkafka {

Broker::Broker(boost::asio::io_service& io_service, std::string host, int32_t port, std::string client_id)
    : client_id_(std::move(client_id))
    , identity_({0, host, port}) // intentionally copy host string again
    , conn_(io_service, std::move(host), port) // move it here
    , send_q_(conn_, [this](std::unique_ptr<RPC> rpc){ recv_q_.push(std::move(rpc)); })
    , recv_q_(conn_, nullptr)
{
}

Broker::~Broker()
{
    close();
}

void Broker::close()
{
     conn_.close();
}

std::future<PacketDecoder> Broker::call(int16_t api_key, std::unique_ptr<PacketEncoder> request_packet)
{
    auto rpc = std::unique_ptr<RPC>(new RPC(api_key, std::move(request_packet), client_id_));

    auto f = rpc->get_future();

    send_q_.push(std::move(rpc));

    return f;
}

std::error_code Broker::connect()
{
    auto boost_ec = conn_.connect();
    if (boost_ec) {
        // Treat all errors in connect as network failures.
        // this might mask some very rare conditions but it's semantically the same
        // thing to client and provides convenient way for them to tell if operations
        // failed for kafka-specific reasons or general transport failure.
        return make_error_code(synkafka_error::network_fail);
    }
    return std::error_code();
}


}