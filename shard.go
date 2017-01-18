package gokc

import (
	k "github.com/sendgridlabs/go-kinesis"
	"github.com/stathat/consistent"
	"log"
	"time"
)

type StreamConsumptionManager interface {
	Start(<-chan LeaseNotification, chan<- Checkpoint)
}

type ShardConsumerFactory func(string) ShardConsumer

func NewKinesisShardConsumptionManager(config *Config, processorFactory MessageProcessorFactory) *KinesisShardConsumptionManager {
	return &KinesisShardConsumptionManager{
		consumerFactory: func(shardId string) ShardConsumer {
			return NewKinesisShardConsumer(config, shardId, processorFactory)
		},
		consumers: map[string]ShardConsumer{},
	}
}

type KinesisShardConsumptionManager struct {
	consumerFactory ShardConsumerFactory
	consumers       map[string]ShardConsumer
}

func (self *KinesisShardConsumptionManager) Start(leaseNotifications <-chan LeaseNotification, checkpoints chan<- Checkpoint) {
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
	Start(string, chan<- Checkpoint)
	Quit()
}

func NewKinesisShardConsumer(config *Config, shardId string, processorFactory MessageProcessorFactory) *KinesisShardConsumer {
	if config.NumProcessorsPerShard < 1 {
		log.Fatal("Need at least 1 processors per shard. Current: ", config.NumProcessorsPerShard)
	}

	return &KinesisShardConsumer{
		processorFactory:         processorFactory,
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
	processorFactory         MessageProcessorFactory
	quit                     chan interface{}
}

func (self *KinesisShardConsumer) loop(checkpoint string, checkpoints chan<- Checkpoint, c *consistent.Consistent, processorMap map[string]chan Message) {
	iteratorType := "TRIM_HORIZON"
	if checkpoint != "" {
		iteratorType = "AFTER_SEQUENCE_NUMBER"
	}
	shardIterator := GetShardIterator(self.conn, self.streamName, self.shardId, iteratorType, checkpoint)
	fetchTicker := time.NewTicker(self.getRecordIntervalSeconds)
	recordsInFlight := 0
	for {
		select {
		case <-self.quit:
			log.Print("Process: ending loop")
			return
		case <-fetchTicker.C:
			if recordsInFlight > 0 {
				log.Println(recordsInFlight, " records in flight.")
				continue
			}
			// TODO: If the interval is too long, the iterator may expire. I wonder if ConsumeShard
			// should be capable of generating the iterator from the last known checkpoint?
			resp := ConsumeShard(self.conn, self.streamName, shardIterator, self.maxRecordsPerFetch)
			if len(resp.Records) == 0 {
				// This is essentially just a heartbeat.
				checkpoints <- &KinesisShardCheckpoint{shardId: self.shardId, sequenceNumber: checkpoint}
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
				recordsInFlight++

				data, err := record.GetData()
				if err != nil {
					log.Print("ERROR: Calling kinesis.GetData() for record. ", err)
					panic("TODO: send retry or fail ack")
				}
				processorChan <- NewBasicMessage(record.SequenceNumber, data)
			}
			shardIterator = resp.NextShardIterator
			if shardIterator == "" {
				log.Print("End reached for ", self.shardId, " ", recordsInFlight)
				checkpoint = SHARD_END
				fetchTicker.Stop()
			}
		case ack := <-finished:
			delete(recordsInFlight, ack.Id)
			log.Println("TODO: react accordingly to ack.Result")
			if len(recordsInFlight) == 0 {
				// Update the shard's checkpoint with the latest sequenceNumber.
				log.Printf("Processing done. Updating checkpoint for %v to %v.\n", self.shardId, checkpoint)
				checkpoints <- &KinesisShardCheckpoint{shardId: self.shardId, sequenceNumber: checkpoint}
			}
		case <-time.After(2 * time.Second):
			// Will fire after the fetchTicker stops.
			if checkpoint == SHARD_END && len(recordsInFlight) == 0 {
				// Send the final checkpoint, and schedule an exit.
				checkpoints <- &KinesisShardCheckpoint{shardId: self.shardId, sequenceNumber: checkpoint}
				self.quit <- true
			}
		}
	}
}

func (self *KinesisShardConsumer) Start(lastCheckpoint string, checkpoints chan<- Checkpoint) {
	c := consistent.New()
	processorMap := map[string]chan Message{}

	successChan := make(chan Shardable, self.maxRecordsPerFetch)
	failureChan := make(chan Message, self.maxRecordsPerFetch)
	retryChan := make(chan Message, self.maxRecordsPerFetch)

	go self.handleResults(successChan, failureChan, retryChan)
	for i := 0; i < self.numProcessors; i++ {
		id := string(i)
		c.Add(id)
		processorMap[id] = make(chan Message, self.maxRecordsPerFetch)
		go self.processorFactory().Start(processorMap[id], successChan, retryChan, failureChan)
	}
	go self.loop(lastCheckpoint, checkpoints, c, processorMap)
}

func (self *KinesisShardConsumer) Quit() {
	self.quit <- true
}

func (self *KinesisShardConsumer) handleResults(success chan Shardable, retry chan Message, failure chan Message) {
	for {
		select {
		case s <- success:
			self.getSuccessWriter().Write(s)
		case f <- failure:
			self.getFailureWriter().Write(f)
		case r <- retry:
			self.getRetryWriter().Write(r)
		}
	}
}
