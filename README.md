# GOKC

GoKC (a GO Kinesis Client) is a framework for creating stream-processing applications using aws infrastructure (kinesis/dynamo/s3), but potentially applicable to any stream producer/consumer.

### Concepts

NOTE: Many of the terms below are borrowed from [Kinesis](http://docs.aws.amazon.com/kinesis/latest/dev/key-concepts.html). Other concepts were borrowed (though don't necessarily share a name) from the [KCL](https://github.com/awslabs/amazon-kinesis-client), which provides similar functionality as this library with two major differences:
  * kcl is written Java, gokc in go.
  * gokc allows multiple processors to act on records from a single-shard

__Stream__: An ordered, shardable sequence of records that can be written to or consumed from. This library was originally developed with Kinesis in mind, but a stream can be thought of as a fancy FIFO queue.

__Shard__: A paritioned stream. See [Kinesis documentation](http://docs.aws.amazon.com/kinesis/latest/dev/key-concepts.html) documentation.

Simple streams may contain just one shard (in which case 'Shard' and 'Stream' are conceptually synonymous). In order to scale horizontally, however, a stream may be split into many shards.

__Lease__: A "read lock" for a particular stream shard. Leases are records of current ownership of a particular shard by a particular stage. Before a Stage can process records from a stream shard, it needs to acquire the lease for that shard.

__Stage__: The conceptual "processor" that transforms messages from an input stream into messages for an output stream. In an over-simplified way, stages are functions that, given an input queue and an output queue, process each input and send to output.

```go
stage := func(in, out IntStream) {
    for {
    	num := in.GetNext("shard1")
		output.Send("shard1", num * 2)
    }
}
```

The less-simplified version is that Stages manage a lot of moving parts. They keep track of shard leases, they setup/tear-down message processors accordingly, and hey handle the retry logic for each record (based the ack from the message processor).

__Message Processor__: The (inventively-named) unit of processing within a stage. Our example stage above is very simple, but think of a message processors as threads of execution within a stage.

Message Processors are what provide sub-shard concurrency. Lets pretend we have a single-shard input stream that we've populated with some records:
```
input = [2, 4, 7]
```

If the stage is configured to use two message processors (MP1 and MP2) which each take about 2 seconds to execute, the order of processing may look like this:
```
Seconds:  0.........1.........2.........3.........4

MP1 :     |-- Process '2' --|-- Process '7' --| 
MP2 :       |-- Process '4' --|
```

NOTE: input records are consistently hashed to a message processor, so an input stream of [8, 8, 8], for example, would all be sent to a single message producer, no matter how many message processors there were.

### Details

Lots of things to flesh out:
  * Stage/Message Processor lifetimes. How startup/shutdown is handled.
  * Error handling (wrapped messages, ephemeral vs. permanent errors)
  * Multiple stages for a single stream.
  * Connectors?
  * How lease acquisition/checkpointing works

### Example

   See [example](https://github.com/cameront/gokc/tree/master/example/example.go)
