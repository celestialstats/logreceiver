package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/davecgh/go-spew/spew"
	"github.com/streadway/amqp"
)

type LogFile struct {
	ServerType string
	Server     string
	logDir     string
	logFiles   map[string]*os.File
	logChannel chan map[string]string
}

var quit = make(chan struct{})
var logDir = flag.String("log-dir", "", "Directory for Log Storage")
var rmqHostname = flag.String("rmq-hostname", "", "RabbitMQ Server Hostname")
var rmqPort = flag.String("rmq-port", "", "RabbitMQ Server Port")
var rmqUsername = flag.String("rmq-username", "", "RabbitMQ Username")
var rmqPassword = flag.String("rmq-password", "", "RabbitMQ Password")
var rmqReceiveQueue = flag.String("rmq-receive-queue", "", "RabbitMQ Receive Queue")
var rmqSendQueue = flag.String("rmq-send-queue", "", "RabbitMQ Send Queue")
var logger *LogFile

func main() {
	log.SetLevel(log.DebugLevel)
	log.Info("--- Celestial Stats Log Receiver ---")
	flag.Parse()
	if *logDir == "" {
		*logDir = os.Getenv("LOGREC_LOG_DIRECTORY")
	}
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
	log.Info("\tLog Directory: ", *logDir)
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

	logger = NewLogWriter(*logDir, 1000)
	go receive(msgs)

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

func receive(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)
		var newEntry = make(map[string]string)
		err := json.Unmarshal(d.Body, &newEntry)
		if err != nil {
			// Do something here for invalid JSON in the queue.
			spew.Dump(err)
		}
		logger.AddEntry(newEntry)
	}
}

// AddEntry adds a map representing the chat message to the log channel. It
// also appends the current Unix Timestamp in milliseconds to the map.
func (logFile *LogFile) AddEntry(newEntry map[string]string) {
	logFile.logChannel <- newEntry
}

// NewLogFile returns a new LogFile ready to recieve log entries and write them to disk.
func NewLogWriter(LogDir string, MaxQueue int) *LogFile {
	cl := &LogFile{
		logDir:     LogDir,
		logFiles:   make(map[string]*os.File),
		logChannel: make(chan map[string]string, MaxQueue),
	}
	go cl.write()
	return cl
}

// Open opens a specific structured file for later writing.
func (logFile *LogFile) open(Timestamp, ServerType, ServerID string) *os.File {
	var logFilename = generateFilename(Timestamp, ServerType, ServerID)
	var parentDir = path.Dir(logFilename)
	log.Debug("Opening Log: ", logFilename)
	if _, err := os.Stat(parentDir); os.IsNotExist(err) {
		log.Debug("\tParent directory doesn't exist. Creating...")
		os.MkdirAll(parentDir, 0755)
	}
	f, err := os.OpenFile(logFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Error opening file: ", logFilename, " - ", err)
	}
	return f
}

// Write outputs any additions to the log channel to the current log file.
// If the log does not exist or is old this triggers the log to be opened
// or rotated. All entries are converted to JSON and stored on object per.
// line.
func (logFile *LogFile) write() {
	for i := range logFile.logChannel {
		curLogFile := logFile.getLogHandle(i["Timestamp"], i["ServerType"], i["ServerID"])
		jsonVal, _ := json.Marshal(i)
		_, err := curLogFile.WriteString(string(jsonVal) + "\n")
		if err != nil {
			log.Fatal("Error writing to file: ", err)
		}
		curLogFile.Sync()
	}
}

// GetLogHandle returns a pointer to the current log file we should be writing to.
func (logFile *LogFile) getLogHandle(Timestamp, ServerType, ServerID string) *os.File {
	var serverKey = ServerType + ServerID
	if _, ok := logFile.logFiles[serverKey]; ok {
		// A log file exists with this server name
		if logFile.logFiles[serverKey].Name() != generateFilename(Timestamp, ServerType, ServerID) {
			// Filename doesn't match where we should be writing so close
			// and re-open with new name
			log.Debug("Closing Log: ", logFile.logFiles[serverKey].Name())
			logFile.logFiles[serverKey].Close()
			logFile.logFiles[serverKey] = logFile.open(Timestamp, ServerType, ServerID)
		}
	} else {
		// Chatlog for this server isn't open, so open it.
		logFile.logFiles[serverKey] = logFile.open(Timestamp, ServerType, ServerID)
	}
	return logFile.logFiles[serverKey]
}

// GenerateFilename returns a filename in the following format
// using the current timestamp:
// $LOGDIR/$PROTOCOL/$SERVER/YYYY/MM/DD/HH.csl
func generateFilename(Timestamp, ServerType, Server string) string {
	timeInt, err := strconv.ParseInt(Timestamp, 36, 64)
	if err != nil {
		// Some error checking here?
		spew.Dump(err)
	}
	timeInt = timeInt * 1000000
	return path.Join(
		*logDir,
		ServerType,
		Server,
		time.Unix(0, timeInt).UTC().Format("2006/01/02/15.csl"))
	//time.Unix(0, timeInt).UTC().Format("2006/01/02/15-04-05.csl"))
}
