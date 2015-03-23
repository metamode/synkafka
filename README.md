= SynKafka: A simple, synchronous C++ producer client API for Apache Kafka =

== Rationale

!! You almost certainly should not use this.

If you want a C/C++ Kafka client use [librdkafka](https://github.com/edenhill/librdkafka).

This library is **incomplete** and potentially **slow**.

It's purpose is as a low-level, simplistic implementation of the Kafka 0.8 (producer) protocol which allows
specific use-cases more control over how they produce and the transactional/error handling semantics.

The motivating case is wanting to write a [Facebook Scribe](https://github.com/facebookarchive/scribe) Store that writes to Kafka cluster in a similar
way to an upstream Scribe server. This is very hard with an asynchronous API like librdkafka where you have
no control over when things flush, how they are batched or how to handle whole/partial failures within batches.

Since Scribe already handles batching and partitioning into topics (and potentially partitions depending on config),
we needed a much lower level API where we can make our own tradeoffs about batching efficiency vs. simplicity in error handling.

For example we might choose to only produce to a single partition in any Produce request such that if that partition is unavailable
we can fail the Store and put it into disk-spooling mode. This reduces throughput.

We do take care to take advantage of Kafka's pipelining in the protocol though such that multiple stores cna be sending batches to same broker
on single connection at same time.

== Limitations

 * Only some of the API is implemented currently - only enough to discover where partitions are and produce to them.
 * API is low-level and requires external work (queuing, multithreading) to get good performance.

== Dependencies

This library aims to build statically with vendored dependencies mostly. 

That said right now it links dynamically with zlib (tested with 1.2.8), and requires boost headers in system path (tested with boost 1.57) just because boost is so
massive to vendor in and we only use a few small headers (asio is vendored separately..)

== Usage

TODO