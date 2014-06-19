package main

import (
	"flag"
	"github.com/cameront/gokc/config"
	"log"
)

type ExampleRecordProcessor struct{}

func (self *ExampleRecordProcessor) StartProcessing(in <-chan []*gokc.Record, ack chan<- string) {
	for {
		select {
		case records, ok := <-in:
			if !ok {
				log.Print("ExampleRecordProcessor: channel closed. Exiting")
				return
			}
			for _, record := range records {
				log.Printf("%+v", string(record.Data))
				ack <- record.SequenceNumber
			}
		}
	}
	log.Print("SimpleRecordProcessor exiting")
}

func main() {
	configFile := flag.String("config", "./config.json", "Config file path.")
	flag.Parse()
	config := config.ParseConfigOrDie(*configFile)
	processorFactory := func() gokc.RecordProcessor { return &ExampleRecordProcessor{} }
	worker := gokc.NewWorker(&config, processorFactory)
	worker.Start()
}
