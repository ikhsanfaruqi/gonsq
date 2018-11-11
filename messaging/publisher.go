package messaging

import (
	"encoding/json"
	"log"

	"github.com/nsqio/go-nsq"
)

// NewPublisher function return Engine struct with given options and configured producer
func NewPublisher(opt Options) Publisher {

	prod, err := nsq.NewProducer(opt.PublishAddress, nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	return Publisher{
		opt:  opt,
		prod: prod,
	}

}

// PublishMessage function publish given data to topic
func (p Publisher) PublishMessage(topic string, data interface{}) error {
	var (
		payload []byte
		err     error
	)
	payload, err = json.Marshal(data)
	if err != nil {
		return err
	}

	topic = p.opt.Prefix + topic

	return p.prod.Publish(topic, payload)
}
