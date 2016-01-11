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

func NewServer() *amqp_thrift.TAMQPServer {
	s, _ := amqp_thrift.NewTAMQPServer(
		test.NewFooProcessor(&Handler{}),
		thrift.NewTJSONProtocolFactory(),
		amqp_thrift.ServerOptions{},
	)

	return s
}

func NewClient() *test.FooClient {
	t, _ := amqp_thrift.NewTAMQPClient(
		amqp_thrift.DefaultAMQPURI,
		amqp_thrift.DefaultExchangeName,
		amqp_thrift.DefaultRoutingKey,
	)

	t.Open()

	return test.NewFooClientFactory(t, thrift.NewTJSONProtocolFactory())
}

func TestYolo(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	s := NewServer()

	go s.Serve()

	err := NewClient().Yolo()
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
	s := NewServer()

	go s.Serve()

	r, err := NewClient().Add(int64(1), int64(1))
	if err != nil {
		t.Errorf("Error happened: %s", err.Error())
	}

	if r != 2 {
		t.Errorf("Wrong result: %d", r)
	}

	s.Stop()
}

func TestPing(t *testing.T) {
	s := NewServer()

	go s.Serve()

	err := NewClient().Ping()

	if _, ok := err.(*test.TestException); !ok {
		t.Errorf("Error happened: %+v", err)
	}

	s.Stop()
}
