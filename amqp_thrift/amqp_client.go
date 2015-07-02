package amqp_thrift

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/upfluence/thrift/lib/go/thrift"
	"io"
	"os"
)

type TAMQPClient struct {
	URI            string
	Connection     *amqp.Connection
	Channel        *amqp.Channel
	QueueName      string
	ExchangeName   string
	RoutingKey     string
	requestBuffer  *bytes.Buffer
	responseReader io.Reader
	deliveries     <-chan amqp.Delivery
}

func NewTAMQPClient(amqpURI, exchangeName, routingKey string) (thrift.TTransport, error) {
	buf := make([]byte, 0, 1024)

	return &TAMQPClient{
		URI:           amqpURI,
		requestBuffer: bytes.NewBuffer(buf),
		ExchangeName:  exchangeName,
		RoutingKey:    routingKey,
	}, nil
}

func (c *TAMQPClient) Open() error {
	var err error

	if c.Connection == nil {
		if c.Connection, err = amqp.Dial(c.URI); err != nil {
			return err
		}
	}

	if c.Channel == nil {
		if c.Channel, err = c.Connection.Channel(); err != nil {
			return err
		}
	}

	queue, err := c.Channel.QueueDeclare(
		"",    // name of the queue
		true,  // durable
		true,  // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)

	if err != nil {
		return err
	}

	c.QueueName = queue.Name

	r, err := NewAMQPQueueReader(c.Channel, c.QueueName, make(chan bool))

	if err != nil {
		return err
	}

	c.responseReader = r

	go r.Consume()

	return nil
}

func (c *TAMQPClient) IsOpen() bool {
	return c.Connection != nil && c.Channel != nil
}

func (c *TAMQPClient) Close() error {
	if c.Connection == nil {
		return errors.New("The connection is not opened")
	}

	defer func() {
		c.Channel = nil
		c.Connection = nil
	}()

	c.Connection.Close()

	return nil
}

func (c *TAMQPClient) Read(buf []byte) (int, error) {
	return c.responseReader.Read(buf)
}

func (c *TAMQPClient) Write(buf []byte) (int, error) {
	return c.requestBuffer.Write(buf)
}

func (c *TAMQPClient) Flush() error {
	err := c.Channel.Publish(
		c.ExchangeName,
		c.RoutingKey,
		false,
		false,
		amqp.Publishing{
			Body:          c.requestBuffer.Bytes(),
			ReplyTo:       c.QueueName,
			CorrelationId: generateUUID(),
		},
	)

	buf := make([]byte, 0, 1024)
	c.requestBuffer = bytes.NewBuffer(buf)
	return err
}

func generateUUID() string {
	f, _ := os.Open("/dev/urandom")
	b := make([]byte, 16)
	f.Read(b)
	f.Close()
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
