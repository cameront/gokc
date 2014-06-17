package gokc

import (
	"encoding/json"
	"log"
)

func ParseData(record *Record, data interface{}) {
	if err := json.Unmarshal(record.Data, data); err != nil {
		log.Fatal("Error parsing record: ", err)
	}
}

func CreateRecords(partitionKey string, sequenceNumber string, data interface{}) []*Record {
	return []*Record{CreateRecord(partitionKey, sequenceNumber, data)}
}

func CreateRecord(partitionKey string, sequenceNumber string, data interface{}) *Record {
	bytes, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("Error creating record: %v %+v.", err, data)
	}
	return &Record{
		PartitionKey:   partitionKey,
		SequenceNumber: sequenceNumber,
		Data:           bytes,
	}
}
