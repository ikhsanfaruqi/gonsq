package messaging

import (
	"errors"
	"log"

	nsq "github.com/bitly/go-nsq"
)

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

// ConsumerEngine struct act as function receiver and main engine to run consumer
// it keep the option and consumers to use
type ConsumerEngine struct {
	opt       Options
	consumers []*nsq.Consumer
}

// NewConsumerEngine function return ConsumerEngine struct with given options
// this can be done also by call ConsumerEngine directly because there's no logic done here
// consumerEngine := ConsumerEngine{opt}
func NewConsumerEngine(opt Options) *ConsumerEngine {
	return &ConsumerEngine{
		opt: opt,
	}
}

// RegisterConsumer function create a new consumer based on given option, and register it to Engine's consumer
func (e *ConsumerEngine) RegisterConsumer(opt ConsumerOptions) error {
	var (
		conf *nsq.Config
		cons *nsq.Consumer
		err  error
	)

	conf = nsq.NewConfig()
	conf.MaxAttempts = opt.MaxAttempts
	conf.MaxInFlight = opt.MaxInFlight

	// add given topic with prefix
	prefixedTopic := e.opt.Prefix + opt.Topic

	cons, err = nsq.NewConsumer(prefixedTopic, opt.Channel, conf)
	if err != nil {
		return err
	}
	cons.AddHandler(opt.Handler)
	e.consumers = append(e.consumers, cons)

	return err

}

// GetConsumersNumber function return registered consumer number
func (e *ConsumerEngine) GetConsumersNumber() int {
	return len(e.consumers)
}

// RunConsumer function connect registered consumer to nsqd
func (e *ConsumerEngine) RunConsumer() {

	if len(e.consumers) == 0 {
		log.Fatal(errors.New("no consumer registered"))
	}

	for _, c := range e.consumers {
		err := c.ConnectToNSQLookupds(e.opt.LookupAddress)
		if err != nil {
			log.Fatal(err)
			break
		}
	}

}
