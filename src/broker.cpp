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
    ,conn_(io_service, host, port)
    ,send_q_(conn_, [this](std::unique_ptr<RPC> rpc){ recv_q_.push(std::move(rpc)); })
    ,recv_q_(conn_, nullptr)
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
    return std_from_boost_ec(conn_.connect());
}


}