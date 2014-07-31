package main

import (
	"flag"
	"github.com/cameront/gokc"
	"log"
)

// ExampleMessageProcessor expects string payloads and always acks Success
type ExampleMessageProcessor struct{}

func (self *ExampleMessageProcessor) Start(messageChan <-chan *gokc.Message, ackChan chan<- gokc.Ack) {
	for {
		select {
		case input, ok := <-messageChan:
			if !ok {
				log.Print("ExampleMessageProcessor: channel closed. Exiting")
				return
			}
			log.Printf("%s\n", string(input.Data))
			ackChan <- gokc.Ack{Id: input.Id, Result: gokc.SUCCESS}
		}
	}
	log.Print("ExampleMessageProcessor exiting")
}

func main() {
	configFile := flag.String("config", "./config.json", "Config file path.")
	flag.Parse()
	config := gokc.MustParseConfig(*configFile)

	processorFactory := func() gokc.MessageProcessor { return &ExampleMessageProcessor{} }
	stage := gokc.NewStage(&config, processorFactory)
	stage.Start()
}
