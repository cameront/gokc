package gokc

// RawRecord is the 'across-the-wire' representation sent/received to/from streams.
type RawRecord struct {
	// ParitionKey determines both the stream shard that the record is sent to (kinesis) as well as
	// the message processor that consumes it (gokc).
	PartitionKey string
	// SequenceNumber is guaranteed by the stream producer to increase across messages.
	SequenceNumber string
	Data           []byte
	// Meta 		*RecordMeta
}

func NewRawRecord(partitionKey, sequenceNumber string, data []byte) *RawRecord {
	return &RawRecord{PartitionKey: partitionKey, SequenceNumber: sequenceNumber, Data: data}
}

// RecordMeta isn't currently used. Idea is to make it transparently part of the stream message, but
// not sure if we should require all clients to wrap their payloads inside of a RecordMeta struct
// in order to even start using gokc? Would be nice to only require that if you want the ability to
// retry a certain number of times.

// RecordMeta contains the message payload along with some metadata about how many times the message
// has previously been attempted, and an info field for storing arbitrary messages (e.g.
// failure/retry reasons).
type RecordMeta struct {
	// The number of times this message has been attempted previously.
	NumRetries int
	Info       string
	Payload    []byte
}
