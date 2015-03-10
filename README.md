= SynKafka: A simple, synchronous C++ producer client API for Apache Kafka =

== Rationale

!! You almost certainly should not use this.

If you want a C/C++ Kafka client use [librdkafka](https://github.com/edenhill/librdkafka).

This library is **incomplete** and **slow**.

It's purpose is as a low-level, simplistic implementation of the Kafka 0.8 protocol which allows
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

== Usage

TODO