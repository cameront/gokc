package gokc

import (
	"encoding/json"
	k "github.com/sendgridlabs/go-kinesis"
	"log"
	"time"
)

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

// ShardLister returns a channel from which all shards will periodically be
// sent.
type ShardLister interface {
	//List() ([]*Shard, error)
	Start(<-chan struct{}) <-chan Shard
}

// NewKinesisShardLister creates and returns a KinesisShardLister
func NewKinesisShardLister(kConfig KinesisConfig) *KinesisShardLister {
	return &KinesisShardLister{
		streamName:          kConfig.StreamName,
		syncIntervalSeconds: time.Duration(kConfig.ShardSyncIntervalSeconds) * time.Second,
		conn:                k.New(kConfig.Auth.AccessKey, kConfig.Auth.SecretKey),
	}
}

// KinesisShardLister periodically returns a list of kinesis shards available
// for processing.
type KinesisShardLister struct {
	streamName          string
	syncIntervalSeconds time.Duration
	conn                k.KinesisClient
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
	args := k.NewArgs()
	args.Add("StreamName", self.streamName)
	for hasMoreShards {
		response, err := self.conn.DescribeStream(args)
		if err != nil {
			return nil, err
		}
		copyShardsInto(response.StreamDescription.Shards, &result)
		if !response.StreamDescription.IsMoreDataAvailable {
			hasMoreShards = false
		} else {
			args.Add("ExclusiveStartShardId", result[len(result)-1].ShardId)
		}
	}
	return result, nil
}

// copyShardsInto copies an array of struct into an array of struct pointers.
// It's a little silly.
func copyShardsInto(src []k.DescribeStreamShards, dest *[]*Shard) error {
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
func fromDescribeStreamShard(other k.DescribeStreamShards) (*Shard, error) {
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

func GetShardIterator(conn k.KinesisClient, streamName, shardId, iteratorType, startingSequenceNumber string) string {
	args := k.NewArgs()
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

func ConsumeShard(conn k.KinesisClient, streamName, shardIterator string, limit int) *k.GetRecordsResp {
	args := k.NewArgs()
	args.Add("ShardIterator", shardIterator)
	args.Add("Limit", limit)
	resp, err := conn.GetRecords(args)
	if err != nil {
		log.Print("ConsumeShard error: ", err)
	}
	return resp
}
