#include <boost/bind.hpp>

#include "connection.h"
#include "log.h"

namespace errc = boost::system::errc;

namespace synkafka
{

Connection::impl::impl(boost::asio::io_service& io_service, std::string host, int32_t port)
    : dns_query_(std::move(host), std::to_string(port))
    , socket_(io_service)
    , strand_(io_service)
    , resolver_(io_service)
    , timeout_ms_(1000)
    , mu_()
    , cv_()
    , state_(STATE_INIT)
    , ec_()
{}

error_code Connection::impl::close(error_code ec, bool lock_held)
{
    std::unique_lock<std::mutex> lk(mu_, std::defer_lock);

    if (!lock_held) {
        // Need to lock ourselves for duration of function
        lk.lock();
    }

    if (state_ == STATE_CLOSED) {
        return ec_;
    }

    if (ec) {
        log()->warn() << "Connection to " << dns_query_ << " closed with error: " << ec.message();
    } else {
        log()->debug() << "Connection to " << dns_query_ << " closed";
        // Must set an error otherwise other threads waiting on connect will just see ec_ is not
        // an error and assume that connection is fine.
        ec = make_error_code(boost::system::errc::connection_aborted);
    }

    ec_ = ec;

    if (state_ == STATE_INIT) {
        state_ = STATE_CLOSED;
        return ec;
    }

    state_ = STATE_CLOSED;

    socket_.shutdown(tcp::socket::shutdown_both, ec);

    // Pass ec - we already copied it's value so close might change local copy
    // but we don't care, saves us allocating another one on stack we don't care about
    socket_.close(ec);

    // Wake up any threads waiting to connect...
    cv_.notify_all();

    return ec_;
}

Connection::Connection(boost::asio::io_service& io_service, std::string host, int32_t port)
    : pimpl_(new impl(io_service, std::move(host), port), [](impl* impl){ impl->close(); delete impl; })
{
}

error_code Connection::connect()
{
    std::unique_lock<std::mutex> lk(pimpl_->mu_);

    switch (pimpl_->state_)
    {
    case STATE_CONNECTED:
        // Already connected, return immediately (with null error)
        log()->debug() << *this << "connect(): is already connected";
        return error_code();

    case STATE_CLOSED:
        // already closed
        log()->debug() << *this << "connect(): is already closed: " << pimpl_->ec_.message();
        return pimpl_->ec_;

    case STATE_CONNECTING:
        {
            // Another thread is already connecting, wait for it to succeed/timeout
            auto ok = pimpl_->cv_.wait_for(lk
                                          ,std::chrono::milliseconds(pimpl_->timeout_ms_)
                                          ,[this]{ return pimpl_->state_ != STATE_CONNECTING; }
                                          );

            if (!ok) {
                // Condition variable timed out waiting for state change
                auto ec = errc::make_error_code(errc::timed_out);
                // Close with error, this will wake any other threads too
                log()->debug() << *this << "connect(): timed out: " << ec.message();
                // We need to call close without releasing lock though since the connect might have succeeded between our timeout
                // wait on the CV and here and unlocking explicitly before calling close allows a race where coroutine continues processing
                // connection and sends RPC on the connection here before we get to close it due to timeout.
                // So we pass lock_held = true direct to close impl so that it will execute within our current lock scope to update state
                return pimpl_->close(ec, true);
            }

            if (pimpl_->state_ != STATE_CONNECTED) {
                // Connection attempt failed, return error
                log()->debug() << *this << "connect(): closed while we waited: "  << pimpl_->ec_.message();
                return pimpl_->ec_;
            }

            // Success!
            log()->debug() << *this << "connect(): OK";
            return error_code();
        }

    case STATE_INIT:
        pimpl_->state_ = STATE_CONNECTING;
        // unlock so we don't deadlock on recursion
        lk.unlock();
        log()->debug() << *this << "connect(): starting connect";
        // Trigger actual connection coroutine
        (*this)();

        return connect();
    }

    // Unreachable but gcc can't figure that out for some reason
    return error_code();
}

// Enable the pseudo-keywords reenter, yield and fork.
#include <boost/asio/yield.hpp>

void Connection::operator()(error_code ec, tcp::resolver::iterator endpoint_iterator)
{
    if (ec == boost::asio::error::operation_aborted) {
        // Failing due to operation being aborted - i.e. socket/resolver was closed externally
        // don't re-enter coroutine in this case as we can't make progress (no point retrying
        // more endpoints for instance)
        // Ensure we signal waiters though and properly close ourselves in case this was triggered
        // by some external destructor for the socket/client etc.
        close(ec);
        return;
    }

    // Coroutine
    reenter (this)
    {
        log()->debug() << *this << "coroutine(): starting resolve";
        yield pimpl_->resolver_.async_resolve(pimpl_->dns_query_, *this);

        if (ec) {
            // Failed to resolve, can't do much with that...
            log()->debug() << *this << "coroutine(): resolve failed: " << ec.message();
            close(ec);
            return;
        }

        while (endpoint_iterator != tcp::resolver::iterator()) {
            log()->debug() << *this << "coroutine(): async connect to "
                << endpoint_iterator->host_name()
                << ":" << endpoint_iterator->service_name();

            yield pimpl_->socket_.async_connect(*endpoint_iterator, boost::bind(*this, _1, endpoint_iterator));

            if (ec) {
                // Error connecting. close socket and try again on next iteration
                auto str = ec.message();
                log()->debug() << *this << "coroutine(): async connect failed: " << ec.message();
                pimpl_->socket_.close();
                ++endpoint_iterator;
            } else {
                // Connected OK, We are done...
                {
                    std::lock_guard<std::mutex> lk(pimpl_->mu_);
                    // Check that we didn't get closed while we were waiting for the lock
                    // this can happen when our connect cv wait times out just slightly before
                    // async call returns so that we are waiting on this lock whilst timeout code is closing the connection.
                    // Continuing blindly leaves us in broken state where we set state back to connected even though we have
                    // cleaned up socket already.
                    if (pimpl_->state_ != STATE_CONNECTING) {
                        // If state changed due to close happening already then just return - other waiters will have been
                        // notified already
                        return;
                    }
                    pimpl_->state_ = STATE_CONNECTED;
                }
                log()->debug() << *this << "coroutine(): async connect OK ";
                // Signal any waiters that we are now connected
                pimpl_->cv_.notify_all();
                return;
            }
        }

        // If we made it here then we faile dto connect to all endpoints given
        // by DNS
        log()->debug() << *this << "coroutine(): async connect failed (no more endpoints): " << ec.message();
        close(ec);
    }
}

// Disable the pseudo-keywords reenter, yield and fork.
#include <boost/asio/unyield.hpp>

error_code Connection::close(error_code ec)
{
    return pimpl_->close(ec);
}

std::ostream& operator<<(std::ostream& os, const Connection& conn)
{
    return os << "Connection[" << conn.pimpl_->dns_query_ << "] ";
}

}