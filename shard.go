package gokc

import (
	k "github.com/sendgridlabs/go-kinesis"
	"github.com/stathat/consistent"
	"log"
	"time"
)

type ShardConsumptionManager interface {
	Start(<-chan LeaseNotification, chan<- ShardCheckpoint)
}

type ShardConsumerFactory func(string) ShardConsumer

func NewKinesisShardConsumptionManager(config *Config, factory MessageProcessorFactory) *KinesisShardConsumptionManager {
	return &KinesisShardConsumptionManager{
		consumerFactory: func(shardId string) ShardConsumer {
			return NewKinesisShardConsumer(config, shardId, factory)
		},
		consumers: map[string]ShardConsumer{},
	}
}

type KinesisShardConsumptionManager struct {
	consumerFactory ShardConsumerFactory
	consumers       map[string]ShardConsumer
}

func (self *KinesisShardConsumptionManager) Start(leaseNotifications <-chan LeaseNotification, checkpoints chan<- ShardCheckpoint) {
	go self.loop(leaseNotifications, checkpoints)
}

func (self *KinesisShardConsumptionManager) loop(leaseNotifications <-chan LeaseNotification, checkpoints chan<- ShardCheckpoint) {
	defer close(checkpoints)
	done := false
	for !done {
		select {
		case leaseNotification, ok := <-leaseNotifications:
			if !ok {
				done = true
				break
			}
			shardId := leaseNotification.Lease.key
			if leaseNotification.Lost {
				consumer, found := self.consumers[shardId]
				if found {
					consumer.Quit()
					delete(self.consumers, shardId)
				} else {
					log.Println("ShardConsumer not found for :", shardId)
				}
			} else {
				consumer := self.consumerFactory(shardId)
				consumer.Start(leaseNotification.Lease.checkpoint, checkpoints)
				log.Print("Consumer started")
				self.consumers[shardId] = consumer
			}
		}
	}
	log.Println("ConsumptionManager: Channel closed. Exiting.")
}

type ShardConsumer interface {
	Start(string, chan<- ShardCheckpoint)
	Quit()
}

func NewKinesisShardConsumer(config *Config, shardId string, factory MessageProcessorFactory) *KinesisShardConsumer {
	if config.NumProcessorsPerShard < 1 {
		log.Fatal("Need at least 1 processors per shard. Current: ", config.NumProcessorsPerShard)
	}

	return &KinesisShardConsumer{
		messageProcessorFactory:  factory,
		numProcessors:            config.NumProcessorsPerShard,
		streamName:               config.Kinesis.StreamName,
		maxRecordsPerFetch:       config.Kinesis.MaxRecordsPerFetch,
		getRecordIntervalSeconds: time.Duration(config.Kinesis.GetRecordsIntervalSeconds) * time.Second,
		shardId:                  shardId,
		conn:                     k.New(config.Kinesis.Auth.AccessKey, config.Kinesis.Auth.SecretKey, k.USWest2),
		quit:                     make(chan interface{}, 1),
	}
}

type KinesisShardConsumer struct {
	streamName               string
	shardId                  string
	numProcessors            int
	maxRecordsPerFetch       int
	getRecordIntervalSeconds time.Duration
	conn                     k.KinesisClient
	messageProcessorFactory  MessageProcessorFactory
	quit                     chan interface{}
}

func (self *KinesisShardConsumer) loop(checkpoint string, checkpoints chan<- ShardCheckpoint, c *consistent.Consistent, processorMap map[string]chan *Message, finished <-chan Ack) {
	iteratorType := "TRIM_HORIZON"
	if checkpoint != "" {
		iteratorType = "AFTER_SEQUENCE_NUMBER"
	}
	shardIterator := GetShardIterator(self.conn, self.streamName, self.shardId, iteratorType, checkpoint)
	recordsInFlight := map[string]bool{}
	fetchTicker := time.NewTicker(self.getRecordIntervalSeconds)
	for {
		select {
		case <-self.quit:
			log.Print("Process: ending loop")
			return
		case <-fetchTicker.C:
			if len(recordsInFlight) > 0 {
				//log.Print("Not finished processing previous batch. Will not fetch records.")
				log.Println(len(recordsInFlight), " records in flight.")
				continue
			}
			// TODO: If the interval is too long, the iterator may expire. I wonder if ConsumeShard
			// should be capable of generating the iterator from the last known checkpoint?
			//			log.Printf("Fetching shard: %s at %s, %s", self.shardId, checkpoint, shardIterator)
			resp := ConsumeShard(self.conn, self.streamName, shardIterator, self.maxRecordsPerFetch)
			//			if len(resp.Records) > 0 {
			//				log.Printf("ConsumeShard returned %d records.", len(resp.Records))
			//			}
			if len(resp.Records) == 0 {
				// This is essentially just a heartbeat.
				checkpoints <- ShardCheckpoint{self.shardId, checkpoint}
			}
			for _, record := range resp.Records {
				processorKey, err := c.Get(record.PartitionKey)
				if err != nil {
					log.Printf("%+v", c.Members())
					log.Print("ERROR getting consistent hash: ", err)
				}
				processorChan, ok := processorMap[processorKey]
				if !ok {
					log.Printf("ERROR: No processor found for partitionKey %s", record.PartitionKey)
				}
				// SequenceNumbers should strictly increase, so we always set the
				// checkpoint to the current sequence number.
				checkpoint = record.SequenceNumber
				recordsInFlight[record.SequenceNumber] = true

				data, err := record.GetData()
				if err != nil {
					log.Print("ERROR: Calling kinesis.GetData() for record. ", err)
					panic("TODO: send retry or fail ack")
				}
				processorChan <- &Message{Id: record.SequenceNumber, Data: data}
			}
			shardIterator = resp.NextShardIterator
			if shardIterator == "" {
				log.Print("End reached for ", self.shardId, " ", len(recordsInFlight))
				checkpoint = SHARD_END
				fetchTicker.Stop()
			}
		case ack := <-finished:
			delete(recordsInFlight, ack.Id)
			log.Println("TODO: react accordingly to ack.Result")
			if len(recordsInFlight) == 0 {
				// Update the shard's checkpoint with the latest sequenceNumber.
				log.Printf("Processing done. Updating checkpoint for %v to %v.\n", self.shardId, checkpoint)
				checkpoints <- ShardCheckpoint{self.shardId, checkpoint}
			}
		case <-time.After(5 * time.Second):
			// Will fire after the fetchTicker stops.
			if checkpoint == SHARD_END && len(recordsInFlight) == 0 {
				// Send the final checkpoint, and schedule an exit.
				checkpoints <- ShardCheckpoint{self.shardId, checkpoint}
				self.quit <- true
			}
		}
	}
}

func (self *KinesisShardConsumer) Start(lastCheckpoint string, checkpoints chan<- ShardCheckpoint) {
	c := consistent.New()
	processorMap := map[string]chan *Message{}
	finished := make(chan Ack, self.maxRecordsPerFetch)
	for i := 0; i < self.numProcessors; i++ {
		id := string(i)
		c.Add(id)
		messageChan := make(chan *Message, self.maxRecordsPerFetch)
		go self.messageProcessorFactory().Start(messageChan, finished)
		processorMap[id] = messageChan
	}
	go self.loop(lastCheckpoint, checkpoints, c, processorMap, finished)
}

func (self *KinesisShardConsumer) Quit() {
	self.quit <- true
}
