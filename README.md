= SynKafka: A simple, synchronous C++ producer client API for Apache Kafka =

== Rationale

!! You almost certainly should not use this.

If you want a C/C++ Kafka client use [librdkafka](https://github.com/edenhill/librdkafka).

This library is **incomplete** and potentially **slow**.

It's purpose is as a low-level, simplistic implementation of the Kafka 0.8 (producer) protocol which allows
specific use-cases more control over how they produce and their transactional/error handling semantics.

The motivating case is wanting to write a [Facebook Scribe](https://github.com/facebookarchive/scribe) Store that writes to Kafka cluster in a similar way to how NetworkStore write to an upstream Scribe server. In particular, the common case of using a BufferStore to write to upstream until upstream fails, spool to disk until upstream is back, replay from disk once upstream is back and finally resume live sending. 

This is very hard to achieve with an asynchronous API like librdkafka where you have no visibility into upstream nodes availability, and can only handle failures per-message and not reason about a whole partition's availability in general.

Since Scribe already handles batching and partitioning into topics (and potentially partitions depending on how you configure it),
we needed a much lower level API where we can make our own tradeoffs about batching efficiency vs. simplicity in error handling.

For example we might choose to only produce to a single partition in any produce request such that if that partition is unavailable
we can fail the KafkaStore and cause an BufferStore into disk-spooling mode. Limiting batches to single partitoin potentially reduces throughput where you have many topics/partitions, but makes Scribe Store's online/offline model sane to work with.

We do take care to take advantage of Kafka's pipelining in the protocol though such that multiple stores (in separate threads) can be sending batches to same broker in parallel, with only a single broker connection held open by the client.

== Limitations

 * Only some of the API is implemented currently - only enough to discover where partitions are and produce to them.
 * API is low-level and requires external work (queuing, multithreading) to get good performance.
 * We did not build this with performance as a primary concern. That said it doesn't have too many pathalogical design choices. In trivial functional tests running on laptop with dockerised 3 node kafka cluster onlocalhost, with batches of 1000 ~60 byte messages, we see sending rates of 100-200k messages a second on aggregate across 8 sending threads. That is without any tuning of messages/thread count let alone proper profiling of code. It well exceeds our current requirements so performance has not been pushed futher.

== Dependencies

 - build system is [tup](http://gittup.org/tup/) as it is simple and fast. 
 - recentish gcc (tested with 4.9.2) (uses `-std=c++11 -stdlib=libc++`)
 - probably other things to have working build chain

This library aims to build statically with vendored dependencies mostly. 

That said some dependencies were more trouble than they are worth to vendor and build as part of project's build system so
we currently assume the following are installed:
 
 - zlib (tested with 1.2.8)
 - boos::asio (headers only but expected in system include path)
 - boost::system

== Building

Clone repo and cd into repo root. From project root dir run:

`tup ./build-debug`

or

`tup ./build-release`

== Running Tests

Build process above will automatically run unit tests.

To run the included functional tests against a local kafka cluster, look at the notes in `./tests/functional/setup_cluster.sh`. This should be run form project root once pre-requisites are met.

This includes `docker-compose` config to build and configure a 3 node kafka cluster with a single zookeeper node.

The setup script can be re-run to automatially reset your local cluster to pristine state.

It must be run before each functional testing session, although individual tests should clean up after themselves so it's not normally required to run it between test runs unless a test aborts and leaves cluster broken or similar.

Once cluster is up, you can run functional tests with:

`$ ./tests/functional/run.py` 

This should be run from the project root and will re-compile debug build automatically if any changes were found.

Runner uses gtest so you can pass normal gtest command options to it, for example:

`$ ./tests/functional/run.py --gtest_filter=ProducerClientTest.*`

== Usage

The public API is intentionally simple, with essentially 2 useful methods and some configuration options.

Simple example:

```

auto client = synkafka::ProducerClient("broker1,broker2:9093");

// call client.set_* to configure or accept defauls (see synkafka.h for details)

// Blocking call - checks if topic/partition is known, and the leader can have TCP connection opened
// If not it returns non-zero std::error_code. See header for more details.
// This is thread safe and many threads can call it even with same partition info concurrently.
auto ec = client.check_topic_partition_leader_available("topic_name", 0);

if (ec) {
	// partition leader not available
}

// Produce a batch of messages
synkafka::MessageSet messages;

// Optionally set compression type, max_message_size config from your Kafka setup (if not using default)
// with messages.set_*

for (std::string& message : your_message_list) {
	auto errc = messages.push(message, /* optional key */ "");
	if (errc == synkafka::synkafka_error::message_set_full) {
		// The message set is full according to Kafka's max_message_size limits
		// you probably want to send it now and continue with you messages in another batch after that
	} else if (errc) {
		// Wild zombies attacked (or some other equally unexpected problem like bad_alloc)
	}
}

// Actually send it. This blocks until we either sent, got an error, or timed out.
// inspect ec to decide which...
ec = client.produce("topic_name", 0 /* partition id */, messages);

```