/*
Package gq is a simple queuing package. It lets you publish a Message to a queue,
and subscribe to the output of that queue elsewhere.

The backend can be either none, in which case go channels are the queue and the service is not distributed,
or a distributed queue service like SQS or RabbitMQ.

Messages can also take a priority level. If the backend does not support message priorities, messages are delivered FIFO.
*/
package gq

import (
	"errors"
	"sync"
	"time"
)

const (
	ErrConnectionUnavailable = "Queue connection unavailable."
	ErrAlreadyOpen           = "Service already open."
)

var (
	brokers = struct {
		sync.RWMutex
		m map[string]Broker
	}{m: make(map[string]Broker)}
	svc *Service
)

// Message is the struct type that gq works with.
type Message struct {
	Body     []byte
	Priority int
	AckFunc  func() error
}

func (m *Message) Ack() error {
	if m.AckFunc != nil {
		return m.AckFunc()
	}
	return nil
}

type Broker interface {
	Open(params *ConnParam) (Connection, error)
}

type Service struct { // equivalent to sql DB
	broker     Broker
	param      *ConnParam
	connection Connection
}

type ConnParam struct {
	Host      string
	Port      int
	UserId    string
	Secret    string
	Heartbeat time.Duration // Server Heartbeat timeout, if applicable to underlying transport
}

func init() {
	svc = New()
}

// Return a new Service instance
func New() (svc *Service) {
	return &Service{}
}

func Open(broker string, conf *ConnParam) (*Service, error) {
	err := svc.Open(broker, conf)
	return svc, err
}

// Set the Service's config and open a connection to the broker.
func (svc *Service) Open(broker string, conf *ConnParam) (err error) {
	if svc.connection != nil {
		return errors.New(ErrAlreadyOpen)
	}
	svc.param = conf
	brokers.RLock()
	svc.broker = brokers.m[broker]
	brokers.RUnlock()
	svc.connection, err = svc.broker.Open(conf)
	return err
}

func Close() { svc.Close() }
func (svc *Service) Close() {
	if svc.connection != nil {
		svc.connection.Close()
		svc.connection = nil
	}
}

// Register a message broker with the gq service
func Register(name string, broker Broker) { svc.Register(name, broker) }
func (svc *Service) Register(name string, broker Broker) {
	brokers.RLock()
	if _, exists := brokers.m[name]; !exists {
		brokers.RUnlock()
		brokers.Lock()
		brokers.m[name] = broker
		brokers.Unlock()
	} else {
		brokers.RUnlock()
	}
}

// Get and Consume functions where noAck is false must return Messages with non-nil
// AckFunc, and then defer closing their Connection/Channel by until the execution of that func.
// Consumers call message.Ack() when they have processed the message.
type Connection interface {
	// Receive one message from the queue service
	GetOne(queue string, noAck bool) (msg Message, err error)

	// Return a channel on which messages will be received
	Consume(queue string, noAck bool) (c chan Message, err error)

	// Send a message to the queue service
	Post(queue string, msg Message, delay time.Duration) (err error)

	// Get the count of outstanding messages on a queue
	Count(queue string) (count int, err error)

	// Delete a message from named queue and message identifier
	Delete(queue string, identifier string) (err error)

	// Close the connection
	Close() (err error)
}

// Get a single message from the named queue, blocking until one is received
func GetMessage(queue string, noAck bool) (msg Message, err error) {
	return svc.GetMessage(queue, noAck)
}
func (svc *Service) GetMessage(queue string, noAck bool) (msg Message, err error) {
	return svc.connection.GetOne(queue, noAck)
}

// Get a channel from which messages can be read.
func Consume(queue string, noAck bool) (c chan Message, err error) { return svc.Consume(queue, noAck) }
func (svc *Service) Consume(queue string, noAck bool) (c chan Message, err error) {
	return svc.connection.Consume(queue, noAck)
}

// Count returns the count of messages on the named queue
func Count(queue string) (count int, err error) { return svc.Count(queue) }
func (svc *Service) Count(queue string) (count int, err error) {
	return svc.connection.Count(queue)
}

// Put a message onto the named queue
func PostMessage(queue string, msg Message, delay time.Duration) (err error) {
	return svc.PostMessage(queue, msg, delay)
}
func (svc *Service) PostMessage(queue string, msg Message, delay time.Duration) (err error) {
	if svc.connection == nil {
		return errors.New(ErrConnectionUnavailable)
	}
	return svc.connection.Post(queue, msg, delay)
}
