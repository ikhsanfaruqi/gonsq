package messaging

import (
	"errors"
	"log"

	"github.com/nsqio/go-nsq"
)

// NewConsumer function return Engine struct with given options and configured consumer
func NewConsumer(opt Options) *Consumer {

	return &Consumer{
		opt: opt,
	}

}

// RegisterConsumer function create a new consumer based on given option, and register it to Engine's consumer
func (e *Consumer) RegisterConsumer(opt ConsumerOptions) error {
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
func (e *Consumer) GetConsumersNumber() int {
	return len(e.consumers)
}

// RunConsumer function connect registered consumer to nsqd
// please note that you have to run this on blocking routine
func (e *Consumer) RunConsumer() {

	if e.GetConsumersNumber() < 1 {
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
