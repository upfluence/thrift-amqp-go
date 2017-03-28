package integration

import (
	"log"
	"testing"
	"time"

	"github.com/upfluence/thrift-amqp-go/Godeps/_workspace/src/github.com/upfluence/thrift/lib/go/thrift"
	"github.com/upfluence/thrift-amqp-go/amqp_thrift"
	"github.com/upfluence/thrift-amqp-go/integration/gen-go/test"
)

var releaseChan = make(chan bool)

type Handler struct{}
type HandlerBis struct{}

func (h *HandlerBis) Add(x, y int64) (int64, error) {
	time.Sleep(1 * time.Second)
	return x + y, nil
}
func (h *HandlerBis) Yolo() error {
	time.Sleep(1 * time.Second)
	releaseChan <- true
	return nil
}

func (h *HandlerBis) Ping() error {
	time.Sleep(1 * time.Second)
	return test.NewTestException()
}

func (h *Handler) Add(x, y int64) (int64, error) {
	return x + y, nil
}

func (h *Handler) Yolo() error {
	releaseChan <- true
	return nil
}

func (h *Handler) Ping() error {
	return test.NewTestException()
}

func NewServer(handler test.Foo, options amqp_thrift.ServerOptions) (*amqp_thrift.TAMQPServer, error) {
	s, err := amqp_thrift.NewTAMQPServer(
		test.NewFooProcessor(handler),
		thrift.NewTJSONProtocolFactory(),
		options,
	)

	if err != nil {
		return nil, err
	}

	return s, err
}

func NewClient(exchangeName string) (*test.FooClient, error) {
	t, err := amqp_thrift.NewTAMQPClient(
		amqp_thrift.DefaultAMQPURI,
		exchangeName,
		amqp_thrift.DefaultRoutingKey,
		50*time.Second,
	)

	if err != nil {
		return nil, err
	}

	err = t.Open()

	if err != nil {
		return nil, err
	}

	return test.NewFooClientFactory(t, thrift.NewTJSONProtocolFactory()), err
}

func TestYolo(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	s, err := NewServer(&Handler{}, amqp_thrift.ServerOptions{ExchangeName: "t1", QueueName: "t1"})

	if err != nil {
		t.Fatal(err)
	}

	go s.Serve()

	c, err := NewClient("t1")

	if err != nil {
		t.Fatal(err)
	}

	err = c.Yolo()

	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-releaseChan:
	case <-time.After(5 * time.Second):
		t.Errorf("Timeout reached")
	}

	s.Stop()
}

func TestAdd(t *testing.T) {
	s, err := NewServer(&Handler{}, amqp_thrift.ServerOptions{ExchangeName: "t2", QueueName: "t2"})

	if err != nil {
		t.Error(err)
	}

	go s.Serve()

	c, err := NewClient("t2")

	if err != nil {
		t.Error(err)
	}

	r, err := c.Add(int64(1), int64(1))

	if err != nil {
		t.Errorf("Error happened: %s", err.Error())
	}

	if r != 2 {
		t.Errorf("Wrong result: %d", r)
	}

	s.Stop()
}

func TestOpenTimeout(t *testing.T) {
	c, err := amqp_thrift.NewTAMQPClient(
		amqp_thrift.DefaultAMQPURI,
		"zozo",
		amqp_thrift.DefaultRoutingKey,
		1*time.Microsecond,
	)

	if err != nil {
		t.Error(err)
	}

	err = c.Open()

	if err == nil {
		t.Errorf("Supposed to timeout")
	}
}

func TestAddTimeout(t *testing.T) {
	s, err := NewServer(&HandlerBis{}, amqp_thrift.ServerOptions{ExchangeName: "t3", QueueName: "t3", Timeout: 40 * time.Millisecond})

	if err != nil {
		t.Error(err)
	}

	go s.Serve()

	c, err := NewClient("t3")

	if err != nil {
		t.Error(err)
	}

	r, err := c.Add(int64(1), int64(1))

	if err != nil {
		t.Errorf("Error happened: %s", err.Error())
	}

	if r != 2 {
		t.Errorf("Wrong result: %d", r)
	}

	s.Stop()
}

func TestPing(t *testing.T) {
	s, err := NewServer(&Handler{}, amqp_thrift.ServerOptions{ExchangeName: "t4", QueueName: "t4"})

	if err != nil {
		t.Fatal(err)
	}

	go s.Serve()

	c, err := NewClient("t4")

	if err != nil {
		t.Error(err)
	}

	err = c.Ping()

	if _, ok := err.(*test.TestException); !ok {
		t.Errorf("Error happened: %+v", err)
	}

	s.Stop()
}
