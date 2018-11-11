package messaging

import "github.com/nsqio/go-nsq"

// Options struct define configuratioin for messaging such as address and prefix
// usually listen and publish adress is same but different on port
// lookup use 4161 while publisher use 4150 for TCP and 4151 for HTTP
// prefix make our message keep organized between tribe
type Options struct {
	LookupAddress  []string
	PublishAddress string
	Prefix         string
}

// ConsumerOptions define configuration use for each nsq consumer
// such as what topic to listen, what channel to use, max attempts and max inflight number of message
type ConsumerOptions struct {
	Topic       string
	Channel     string
	Handler     nsq.HandlerFunc
	MaxAttempts uint16
	MaxInFlight int
}

// Consumer struct act as function receiver and main engine to run consumer
// it keep the option and consumers to use
type Consumer struct {
	opt       Options
	consumers []*nsq.Consumer
}

// Publisher struct act as function receiver and main engine to run publisher
type Publisher struct {
	opt  Options
	prod *nsq.Producer
}
