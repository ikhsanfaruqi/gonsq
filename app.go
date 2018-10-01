package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	nsq "github.com/bitly/go-nsq"
	"github.com/ikhsanfaruqi/gonsq/messaging"
)

const (
	defaultConsumerMaxAttempts = 10
	defaultConsumerMaxInFlight = 100
)

func main() {

	// declare var(s)
	var (
		messagingOptions messaging.Options
		consumerEngine   *messaging.ConsumerEngine
		err              error
	)

	// create messaging options
	messagingOptions = messaging.Options{
		LookupAddress:  []string{"http://devel-go.tkpd:4161"}, //TODO : change this with nsqd address :4161
		PublishAddress: "devel-go.tkpd:4150",                  //TODO : change this with nsqd address :4150
		Prefix:         "tech_curiculum_nsq_",
	}
	consumerEngine = messaging.NewConsumerEngine(messagingOptions)

	// create consumer's option
	consumerOption1 := messaging.ConsumerOptions{
		Topic:       "t1",    // TODO : change this with your topic name
		Channel:     "loger", //TODO : change this with your channel name
		Handler:     handlerConsumer1,
		MaxAttempts: defaultConsumerMaxAttempts,
		MaxInFlight: defaultConsumerMaxInFlight,
	}

	// register consumer 1
	err = consumerEngine.RegisterConsumer(consumerOption1)
	if err != nil {
		log.Fatal(err)
	}

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
	log.Println("got message :", string(msg.Body))
	msg.Finish()
	return nil
}
