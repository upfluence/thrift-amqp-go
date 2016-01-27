package integration

import (
	"log"
	"testing"
	"time"

	"github.com/upfluence/thrift-amqp-go/amqp_thrift"
	"github.com/upfluence/thrift-amqp-go/integration/gen-go/test"
	"github.com/upfluence/thrift/lib/go/thrift"
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

func NewServer(handler test.Foo, options amqp_thrift.ServerOptions) *amqp_thrift.TAMQPServer {
	s, _ := amqp_thrift.NewTAMQPServer(
		test.NewFooProcessor(handler),
		thrift.NewTJSONProtocolFactory(),
		options,
	)

	return s
}

func NewClient(exchangeName string) *test.FooClient {
	t, _ := amqp_thrift.NewTAMQPClient(
		amqp_thrift.DefaultAMQPURI,
		exchangeName,
		amqp_thrift.DefaultRoutingKey,
	)

	t.Open()

	return test.NewFooClientFactory(t, thrift.NewTJSONProtocolFactory())
}

func TestYolo(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	s := NewServer(&Handler{}, amqp_thrift.ServerOptions{ExchangeName: "t1", QueueName: "t1"})

	go s.Serve()

	err := NewClient("t1").Yolo()
	if err != nil {
		t.Errorf("Error happened: %s", err.Error())
	}

	select {
	case <-releaseChan:
	case <-time.After(30 * time.Second):
		t.Errorf("Timeout reached")
	}

	s.Stop()
}

func TestAdd(t *testing.T) {
	s := NewServer(&Handler{}, amqp_thrift.ServerOptions{ExchangeName: "t2", QueueName: "t2"})

	go s.Serve()

	r, err := NewClient("t2").Add(int64(1), int64(1))
	if err != nil {
		t.Errorf("Error happened: %s", err.Error())
	}

	if r != 2 {
		t.Errorf("Wrong result: %d", r)
	}

	s.Stop()
}

func TestAddTimeout(t *testing.T) {
	s := NewServer(&HandlerBis{}, amqp_thrift.ServerOptions{ExchangeName: "t3", QueueName: "t3", Timeout: 40 * time.Millisecond})

	go s.Serve()

	r, err := NewClient("t3").Add(int64(1), int64(1))
	if err != nil {
		t.Errorf("Error happened: %s", err.Error())
	}

	if r != 2 {
		t.Errorf("Wrong result: %d", r)
	}

	s.Stop()
}

func TestPing(t *testing.T) {
	s := NewServer(&Handler{}, amqp_thrift.ServerOptions{ExchangeName: "t4", QueueName: "t4"})

	go s.Serve()

	err := NewClient("t4").Ping()

	if _, ok := err.(*test.TestException); !ok {
		t.Errorf("Error happened: %+v", err)
	}

	s.Stop()
}
