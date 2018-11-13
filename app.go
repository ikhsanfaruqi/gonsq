package main

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ikhsanfaruqi/gonsq/messaging"
	"github.com/nsqio/go-nsq"
)

const (
	defaultConsumerMaxAttempts = 10
	defaultConsumerMaxInFlight = 100
	secondTopicName            = "ikhsan"
)

var publisher messaging.Publisher

func main() {

	// declare var(s)
	var (
		messagingOptions messaging.Options
		consumerEngine   *messaging.Consumer
		err              error
	)

	// create messaging options
	messagingOptions = messaging.Options{
		LookupAddress:  []string{"devel-go.tkpd:4161"}, //TODO : change this with nsqd address :4161
		PublishAddress: "devel-go.tkpd:4150",           //TODO : change this with nsqd address :4150
		Prefix:         "tech_cur_nsq_",
	}
	consumerEngine = messaging.NewConsumer(messagingOptions)

	// create consumer's option
	consumerOption1 := messaging.ConsumerOptions{
		Topic:       "hello",  // TODO : change this with your topic name
		Channel:     "ikhsan", //TODO : change this with your channel name
		Handler:     handlerConsumer1,
		MaxAttempts: defaultConsumerMaxAttempts,
		MaxInFlight: defaultConsumerMaxInFlight,
	}

	consumerOption2 := messaging.ConsumerOptions{
		Topic:       secondTopicName,
		Channel:     "bebase",
		MaxAttempts: defaultConsumerMaxAttempts,
		MaxInFlight: defaultConsumerMaxInFlight,
		Handler:     handlerConsumer2,
	}

	// register consumer 1
	err = consumerEngine.RegisterConsumer(consumerOption1)
	if err != nil {
		log.Fatal(err)
	}

	// register consumer 2
	err = consumerEngine.RegisterConsumer(consumerOption2)
	if err != nil {
		log.Fatal(err)
	}

	publisher = messaging.NewPublisher(messagingOptions)

	// check if consumer registered
	log.Println(consumerEngine.GetConsumersNumber())

	// run the consumer engine
	consumerEngine.RunConsumer()

	// create term so the app didn't exit
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-term:
		log.Println("😥 Signal terminate detected")
	}

}

func handlerConsumer1(msg *nsq.Message) error {
	//TODO : log the message and finish it !

	if msg.Attempts > defaultConsumerMaxAttempts {
		log.Printf("message id : %d finished :", msg.ID)
	}

	if msg.Attempts < 3 {
		log.Println("message error, we have to requeue this")
		return errors.New("lol this is error")
	}

	err := publisher.PublishMessage(secondTopicName, string(msg.Body))
	if err != nil {
		log.Println(err)
	}

	msg.Finish()

	return nil
}

func handlerConsumer2(msg *nsq.Message) error {
	log.Println("got message republished! , value is :", string(msg.Body))
	msg.Finish()
	return nil
}
