package gokc

import (
	"log"
	"os"
	"os/signal"
	"time"
)

func NewStage(config *Config, processor MessageProcessor) *Stage {
	config.Init()
	stage := &Stage{}
	stage.init(config, processor)
	return stage
}

type Stage struct {
	leaseTaker   LeaseTaker
	leaseRenewer LeaseRenewer
	shardManager StreamConsumptionManager
	shardLister  ShardLister
}

func (self *Stage) init(config *Config, processor MessageProcessor) {
	InitDynamoConfig(config)
	if config.LeaseTable.Create {
		self.createAndWaitForTable(config)
	}
	self.leaseTaker = NewDynamoLeaseTaker(config)
	self.leaseRenewer = NewDynamoLeaseRenewer(config)
	self.shardLister = NewKinesisShardLister(config.Kinesis)
	self.shardManager = NewKinesisShardConsumptionManager(config, processor)
}

func (self *Stage) createAndWaitForTable(conf *Config) {
	log.Print("Creating table...")
	leaseManager := NewDynamoLeaseManager(conf)
	if err := leaseManager.CreateTable(conf.LeaseTable.ReadCapacity, conf.LeaseTable.WriteCapacity); err != nil {
		panic(err)
	}
	pollSeconds := time.Duration(conf.LeaseTable.CreatePollIntervalSeconds) * time.Second
	timeoutSeconds := time.Duration(conf.LeaseTable.CreateTimeoutSeconds) * time.Second
	if err := leaseManager.WaitForTable(pollSeconds, timeoutSeconds); err != nil {
		panic(err)
	}
	log.Print("Created table.")
}

func waitToQuit(quit chan struct{}, checkpoints chan Checkpoint) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	// Block until a signal is received.
	select {
	case s := <-c:
		log.Println("Worker: got signal:", s)
		// TODO: Also close if all processing has stopped. But don't consume shardCheckpoints otherwise.
	}
	close(quit)
	for _, ok := <-checkpoints; ok; {
		// Wait until checkpoints (the last stream) is closed by consumption manager
	}
}

func (self *Stage) Start() {
	quit := make(chan struct{})
	checkpoints := make(chan Checkpoint, 10) // Owned/closed by the consumption manager

	shards := self.shardLister.Start(quit)
	newLeases := self.leaseTaker.Start(shards)
	leaseNotifications := self.leaseRenewer.Start(newLeases, checkpoints)
	self.shardManager.Start(leaseNotifications, checkpoints)

	waitToQuit(quit, checkpoints)
	log.Print("Stage: Exiting.")
}
