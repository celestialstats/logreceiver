package main

import (
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
)

var quit = make(chan struct{})
var rmqHostname = flag.String("rmq-hostname", "", "RabbitMQ Server Hostname")
var rmqPort = flag.String("rmq-port", "", "RabbitMQ Server Port")
var rmqUsername = flag.String("rmq-username", "", "RabbitMQ Username")
var rmqPassword = flag.String("rmq-password", "", "RabbitMQ Password")
var rmqReceiveQueue = flag.String("rmq-receive-queue", "", "RabbitMQ Receive Queue")
var rmqSendQueue = flag.String("rmq-send-queue", "", "RabbitMQ Send Queue")

func main() {
	log.SetLevel(log.DebugLevel)
	log.Info("--- Celestial Stats Log Receiver ---")
	flag.Parse()
	if *rmqHostname == "" {
		*rmqHostname = os.Getenv("LOGREC_RABBITMQ_HOSTNAME")
	}
	if *rmqPort == "" {
		*rmqPort = os.Getenv("LOGREC_RABBITMQ_PORT")
	}
	if *rmqUsername == "" {
		*rmqUsername = os.Getenv("LOGREC_RABBITMQ_USERNAME")
	}
	if *rmqPassword == "" {
		*rmqPassword = os.Getenv("LOGREC_RABBITMQ_PASSWORD")
	}
	if *rmqReceiveQueue == "" {
		*rmqReceiveQueue = os.Getenv("LOGREC_RABBITMQ_RECEIVE_QUEUE")
	}
	if *rmqSendQueue == "" {
		*rmqSendQueue = os.Getenv("LOGREC_RABBITMQ_SEND_QUEUE")
	}
	log.Info("Launch Parameters:")
	log.Info("\tRabbitMQ Hostname: ", *rmqHostname)
	log.Info("\tRabbitMQ Port: ", *rmqPort)
	log.Info("\tRabbitMQ Username: ", *rmqUsername)
	log.Info("\tRabbitMQ Password: ", *rmqPassword)
	log.Info("\tRabbitMQ Receive Queue: ", *rmqReceiveQueue)
	log.Info("\tRabbitMQ Send Queue: ", *rmqSendQueue)

	conn, err := amqp.Dial(fmt.Sprintf(
		"amqp://%v:%v@%v:%v/",
		*rmqUsername,
		*rmqPassword,
		*rmqHostname,
		*rmqPort,
	))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		*rmqReceiveQueue, // queue
		"",               // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Info("Log Receiver is now running.  Press CTRL-C to exit.")

	<-quit

	log.Info("Exiting...")
	return
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
