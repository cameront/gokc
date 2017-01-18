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

type Message interface {
	Id() string
	Data() []byte
}

func NewBasicMessage(id string, data []byte) *BasicMessage {
	return &BasicMessage{id: id, data: data}
}

type BasicMessage struct {
	id   string
	data []byte
}

func (b *BasicMessage) Id() string   { return b.id }
func (b *BasicMessage) Data() []byte { return b.data }

type MessageProcessor interface {
	Start(input chan Message, success chan Shardable, retry chan Message, failure chan Message)
}

type MessageProcessorFactory func() MessageProcessor
