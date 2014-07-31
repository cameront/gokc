package gokc

import (
	"encoding/json"
	sgk "github.com/sendgridlabs/go-kinesis"
	"log"
	"time"
)

type StreamWriter interface {
	Write(partitionKey string, data interface{}) error
}

// Shard is a mirror of sendgridlabs/go-kinesis.Shard, but one that we control.
type Shard struct {
	AdjacentParentShardId string
	HashKeyRange          struct {
		EndingHashKey   string
		StartingHashKey string
	}
	ParentShardId       string
	SequenceNumberRange struct {
		EndingSequenceNumber   string
		StartingSequenceNumber string
	}
	ShardId string
}

// ShardCheckpoint is ...
type ShardCheckpoint struct {
	ShardId        string
	SequenceNumber string
}

// ShardLister returns a channel to which all shards will periodically be sent.
type ShardLister interface {
	Start(<-chan struct{}) <-chan Shard
}

// NewKinesisShardLister creates and returns a KinesisShardLister
func NewKinesisShardLister(kConfig KinesisConfig) *KinesisShardLister {
	return &KinesisShardLister{
		streamName:          kConfig.StreamName,
		syncIntervalSeconds: time.Duration(kConfig.ShardSyncIntervalSeconds) * time.Second,
		conn:                sgk.New(kConfig.Auth.AccessKey, kConfig.Auth.SecretKey, sgk.Region{kConfig.Region}),
	}
}

// KinesisShardLister periodically returns a list of kinesis shards available
// for processing.
type KinesisShardLister struct {
	streamName          string
	syncIntervalSeconds time.Duration
	conn                sgk.KinesisClient
}

func (self *KinesisShardLister) loop(quit <-chan struct{}, allShards chan<- Shard) {
	defer close(allShards)
	// Do the first sync after the lease taker has a chance to read the lease
	// table.
	syncTimer := time.NewTimer(10 * time.Second).C
	done := false
	for !done {
		select {
		case <-syncTimer:
			shards, err := self.List()
			if err != nil {
				log.Println("List: Error. ", err)
				done = true
			}
			log.Printf("List: Found %d shards", len(shards))
			for _, shard := range shards {
				allShards <- *shard
			}
			// Do subsequent syncs after the sync interval.
			syncTimer = time.NewTimer(self.syncIntervalSeconds).C
		case <-quit:
			done = true
		}
	}
	log.Print("List: Exiting.")
}

// Start creates a goroutine
func (self *KinesisShardLister) Start(quit <-chan struct{}) <-chan Shard {
	shardChan := make(chan Shard, 10)
	go self.loop(quit, shardChan)
	return shardChan
}

func (self *KinesisShardLister) List() ([]*Shard, error) {
	hasMoreShards := true
	result := []*Shard{}
	args := sgk.NewArgs()
	args.Add("StreamName", self.streamName)
	for hasMoreShards {
		response, err := self.conn.DescribeStream(args)
		if err != nil {
			return nil, err
		}
		copyShardsInto(response.StreamDescription.Shards, &result)
		if !response.StreamDescription.HasMoreShards {
			hasMoreShards = false
		} else {
			args.Add("ExclusiveStartShardId", result[len(result)-1].ShardId)
		}
	}
	return result, nil
}

// copyShardsInto copies an array of struct into an array of struct pointers.
// It's a little silly.
func copyShardsInto(src []sgk.DescribeStreamShards, dest *[]*Shard) error {
	for _, other := range src {
		shard, err := fromDescribeStreamShard(other)
		if err != nil {
			return err
		}
		*dest = append(*dest, shard)
	}
	return nil
}

// terrible way to reassign ownership from their shard to ours, but hopefully
// this doesn't happen often enough to notice.
func fromDescribeStreamShard(other sgk.DescribeStreamShards) (*Shard, error) {
	shard := Shard{}
	bytes, err := json.Marshal(other)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &shard)
	if err != nil {
		return nil, err
	}
	return &shard, nil
}

// args := kinesis.NewArgs()
// args.Add("ShardIterator", shardIterator)
// recResp, err := conn.GetRecords(args)
// if err != nil {
// 	log.Fatal("CaP1: ", err)
// }
// log.Printf("Got %d records.", len(recResp.Records))
// for _, d := range recResp.Records {

func GetShardIterator(conn sgk.KinesisClient, streamName, shardId, iteratorType, startingSequenceNumber string) string {
	args := sgk.NewArgs()
	args.Add("StreamName", streamName)
	args.Add("ShardId", shardId)
	args.Add("ShardIteratorType", iteratorType)
	if startingSequenceNumber != "" {
		args.Add("StartingSequenceNumber", startingSequenceNumber)
	}
	resp, err := conn.GetShardIterator(args)
	if err != nil {
		log.Fatal(err)
	}
	return resp.ShardIterator
}

func ConsumeShard(conn sgk.KinesisClient, streamName, shardIterator string, limit int) *sgk.GetRecordsResp {
	args := sgk.NewArgs()
	args.Add("ShardIterator", shardIterator)
	args.Add("Limit", limit)
	resp, err := conn.GetRecords(args)
	if err != nil {
		log.Print("ConsumeShard error: ", err)
	}
	return resp
}

//
// KinesisStreamWriter
//

func NewKinesisWriter(config KinesisConfig) *KinesisWriter {
	conn := sgk.New(config.Auth.AccessKey, config.Auth.SecretKey, sgk.Region{config.Region})
	return &KinesisWriter{
		client:     conn,
		streamName: config.StreamName,
	}
}

type KinesisWriter struct {
	client     sgk.KinesisClient
	streamName string
}

func ConstructArgs(streamName string, partitionKey string, entity interface{}) (*sgk.RequestArgs, error) {
	args := sgk.NewArgs()
	args.Add("StreamName", streamName)
	bytes, err := json.Marshal(entity)
	if err != nil {
		return nil, err
	}
	args.Add("PartitionKey", partitionKey)
	args.AddData(bytes)
	return args, nil
}

func (self *KinesisWriter) Write(partitionKey string, entity interface{}) error {
	args, err := ConstructArgs(self.streamName, partitionKey, entity)
	if err != nil {
		return err
	}
	_, err = self.client.PutRecord(args)
	return err
}
