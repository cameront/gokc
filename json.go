package gokc

import (
	"encoding/json"
	"fmt"
	"log"
)

// Shardable is for anything that produces shard keys.
type Shardable interface {
	PartitionKey() string
}

// Consider providing shardable as a channel, instead of needing to separate
// out the single/multi processor case here, and have an adapter,
// which kind of sucks.
type TypedProcessor interface {
	Process(in interface{}) (Shardable, bool)
	InputFactory() interface{}
}

// Maybe processors should return an error, and we could check here if that error
// is ephemeral, and retry if so?

type TypedMultiProcessor interface {
	Process(in interface{}) ([]Shardable, bool)
	InputFactory() interface{}
	Type() string
}

type MultiAdapter struct {
	Single TypedProcessor
}

func (m *MultiAdapter) Process(in interface{}) ([]Shardable, bool) {
	out, success := m.Single.Process(in)
	if out == nil {
		return []Shardable{}, success
	} else {
		return []Shardable{out}, success
	}
}

func (m *MultiAdapter) InputFactory() interface{} {
	return m.Single.InputFactory()
}

func (m *MultiAdapter) Type() string {
	return fmt.Sprintf("%+T", m.Single)
}

type JsonProcessor struct {
	typedProcessor TypedMultiProcessor
	successStream  StreamWriter
	retryStream    StreamWriter
	failStream     StreamWriter
}

func (self *JsonProcessor) succeed(next Shardable) error {
	return sendTo(self.successStream, "success", next.PartitionKey(), next)
}

func (self *JsonProcessor) retry(record *RawRecord) error {
	return sendTo(self.retryStream, "retry", record.PartitionKey, record.Data)
}

func (self *JsonProcessor) fail(record *RawRecord) error {
	return sendTo(self.failStream, "fail", record.PartitionKey, record.Data)
}

func sendTo(writer StreamWriter, streamName, partitionKey string, data interface{}) error {
	if writer == nil {
		log.Fatalf("Attempting to writing to %s stream that was never configured!", streamName)
	}
	return writer.Write(partitionKey, data)
}

func (self *JsonProcessor) StartProcessing(in <-chan []*RawRecord, ack chan<- string) {
	for {
		select {
		case records, ok := <-in:
			if !ok {
				log.Print("Channel closed. Exiting")
				return
			}
			for _, record := range records {
				// TODO: Break loop body into its own body.
				input := self.typedProcessor.InputFactory()
				err := json.Unmarshal(record.Data, input)
				if err != nil {
					log.Printf("Krules: failed to parse entity. %s.\n%s", err, string(record.Data))
					err = self.fail(record)
				} else {
					// We succesfully parsed the entity.
					output, success := self.typedProcessor.Process(input)
					if success {
						for _, nextInput := range output {
							if err = self.succeed(nextInput); err != nil {
								log.Printf("Krules - failed to succeed entity: %s\n%s", err, string(record.Data))
								break
							}
						}
					}
					if err != nil || !success {
						// NOTE: This will attempt to retry records that succeeded but then
						// failed to be written to the success stream. Desired?
						// TODO
						// If the err is "ephemeral" write to retry.
						// Otherwise, write to failure.
						log.Println("Attempting to retry.")
						err = self.retry(record)
					}
				}
				if err != nil {
					// Welp, we tried, but not much we can do if we can't succeed/fail/retry!
					log.Fatalf("Unable to proceed in pipeline due to error: %s", err)
				} else {
					ack <- record.SequenceNumber
				}
			}
		}
	}
	log.Print("JsonProcessor exiting")
}

// func StartWithConfig(configPath string, single TypedProcessor) {
// 	StartMultiWithConfig(configPath, &MultiAdapter{single})
// }

// func StartMultiWithConfig(configPath string, multi TypedMultiProcessor) {
// 	conf := struct {
// 		SuccessStream *KinesisConfig
// 		RetryStream   *KinesisConfig
// 		FailureStream *KinesisConfig
// 		Gokc          Config
// 	}{}
// 	if err := ParseConfigOrDie(configPath, &conf); err != nil {
// 		log.Fatal("Unable to parse config from ", configPath)
// 	}
// 	log.SetPrefix(conf.Gokc.AppName + " ")

// 	processorFactory := func() MessageProcessor {
// 		return &JsonProcessor{
// 			typedProcessor: multi,
// 			successStream:  NewKinesisWriter(*conf.SuccessStream),
// 			retryStream:    NewKinesisWriter(*conf.FailureStream),
// 			failStream:     NewKinesisWriter(*conf.FailureStream),
// 		}
// 	}

// 	worker := NewStage(&conf.Gokc, processorFactory)
// 	worker.Start()
// }
