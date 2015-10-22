package amqp_thrift

import (
	"errors"
	"github.com/streadway/amqp"
	"github.com/upfluence/thrift/lib/go/thrift"
	"sync"
)

const ExchangeType string = "direct"

type TServerAMQP struct {
	URI          string
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	QueueName    string
	ExchangeName string
	RoutingKey   string
	deliveries   <-chan amqp.Delivery
	mu           sync.RWMutex
	interrupted  bool
	Options      *ServerOptions
}

type ServerOptions struct {
	Prefetch uint
}

func NewTServerAMQP(
	amqpURI, exchangeName, routingKey, queueName string,
	opts *ServerOptions,
) (*TServerAMQP, error) {
	return &TServerAMQP{
		URI:          amqpURI,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		QueueName:    queueName,
		Options:      opts,
	}, nil
}

func (s *TServerAMQP) Listen() error {
	var err error

	if s.Connection == nil {
		if s.Connection, err = amqp.Dial(s.URI); err != nil {
			return err
		}
	}

	if s.Channel == nil {
		if s.Channel, err = s.Connection.Channel(); err != nil {
			return err
		}
	}

	if opts := s.Options; opts != nil && opts.Prefetch != 0 {
		if err := s.Channel.Qos(int(opts.Prefetch), 0, false); err != nil {
			return err
		}
	}

	if err = s.Channel.ExchangeDeclare(
		s.ExchangeName, // name osf the exchange
		ExchangeType,   // type
		false,          // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return err
	}

	if _, err = s.Channel.QueueDeclare(
		s.QueueName, // name of the queue
		true,        // durable
		false,       // delete when usused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	); err != nil {
		return err
	}

	if err = s.Channel.QueueBind(
		s.QueueName,    // name of the queue
		s.RoutingKey,   // bindingKey
		s.ExchangeName, // sourceExchange
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return err
	}

	s.deliveries, err = s.Channel.Consume(
		s.QueueName, // name
		"",          // consumerTag,
		false,       // noAck
		false,       // exclusive
		false,       //            noLocal
		false,       // noWait
		nil,         // arguments
	)

	if err != nil {
		return err
	}

	return nil
}

func (s *TServerAMQP) Close() error {
	if s.Connection == nil {
		return errors.New("The connection is not opened")
	}

	defer func() {
		s.Channel = nil
		s.Connection = nil
	}()

	s.Connection.Close()

	return nil
}

func (s *TServerAMQP) Accept() (thrift.TTransport, error) {
	s.mu.RLock()
	interrupted := s.interrupted
	s.mu.RUnlock()

	if interrupted {
		return nil, errors.New("Transport Interrupted")
	}
	return NewTAMQPDelivery(<-s.deliveries, s.Channel)
}

func (s *TServerAMQP) Interrupt() error {
	s.mu.Lock()
	s.interrupted = true
	s.mu.Unlock()

	return nil
}
