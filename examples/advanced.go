package main

import (
	"flag"
	"github.com/cameront/gokc"
	"log"
)

func Example(string input, success chan<- gokc.Shardable, fail chan<- gokc.Message, retry chan<- gokc.Message) {
	for {
		select {
		case input, ok := <-messageChan:
			if !ok {
				log.Print("ExampleMessageProcessor: channel closed. Exiting")
				return
			}
			log.Printf("%s\n", string(input.Data))
			success <- gokc.Ack{Id: input.Id, Result: gokc.SUCCESS}
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
