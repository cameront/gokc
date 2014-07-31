package gokc

type AckResult int

const (
	SUCCESS AckResult = 1
	RETRY   AckResult = 2
	FAIL    AckResult = 3
)

type Ack struct {
	Id     string
	Result AckResult
}

type Message struct {
	Id   string
	Data []byte
}

type MessageProcessor interface {
	Start(<-chan *Message, chan<- Ack)
}

type MessageProcessorFactory func() MessageProcessor
