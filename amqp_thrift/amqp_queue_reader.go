package amqp_thrift

import (
	"bytes"
	"github.com/streadway/amqp"
	"sync"
)

type AMQPQueueReader struct {
	Channel   *amqp.Channel
	QueueName string
	exitChan  chan bool
	newData   chan bool
	buffer    *bytes.Buffer
	mutex     *sync.Mutex
}

func NewAMQPQueueReader(channel *amqp.Channel, queueName string, exitChan chan bool) (*AMQPQueueReader, error) {
	return &AMQPQueueReader{
		channel,
		queueName,
		exitChan,
		make(chan bool),
		bytes.NewBuffer(make([]byte, 0, 1024)),
		&sync.Mutex{},
	}, nil
}

func (r *AMQPQueueReader) Consume() error {
	deliveries, err := r.Channel.Consume(
		r.QueueName, // name
		"",          // consumerTag,
		true,        // noAck
		false,       // exclusive
		false,       //            noLocal
		false,       // noWait
		nil,         // arguments
	)

	if err != nil {
		return err
	}

	for {
		select {
		case delivery := <-deliveries:
			shouldNotify := false

			if r.buffer.Len() == 0 {
				shouldNotify = true
			}

			r.buffer.Write(delivery.Body)

			if shouldNotify {
				r.newData <- shouldNotify
			}
		case <-r.exitChan:
			break
		}
	}

	return nil
}

func (r *AMQPQueueReader) Read(b []byte) (int, error) {
	if r.buffer.Len() == 0 {
		<-r.newData
	}

	return r.buffer.Read(b)
}
