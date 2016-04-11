/*
Package gq is a simple queuing package. It lets you publish a []byte - typically json output - to a queue,
and subscribe to the output of that queue elsewhere. The queue can also take a priority level.

The backend can be either none, in which case go channels are the queue and the service is not distributed,
or a distributed queue service like SQS or RabbitMQ.

Note that prioritized queues are, at least in SQS, implemented by creating multiple queues with variant names
whose suffixes indicate the priority level they carry. Consuming directly from an identically named
queue to one of the priority queues may not result in the desired behavior.
*/
package gq

import (
	"sync"
	"time"
)

var (
	drivers = struct {
		sync.RWMutex
		m map[string]Driver
	}{m: make(map[string]Driver)}
	svc *Service = &Service{}
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

type Driver interface {
	Open(params *ConnParam) (Connection, error)
}

type Service struct { // equivalent to sql DB
	driver     Driver
	param      *ConnParam
	connection Connection
}

func init() {
	svc = New()
}

// Return a new Service instance
func New() (svc *Service) {
	return &Service{}
}

// gets called by the user; sets config for the chosen service
func Open(driver string, conf *ConnParam) *Service {
	svc.Open(driver, conf)
	return svc
}
func (svc *Service) Open(driver string, conf *ConnParam) {
	svc.param = conf
	// TODO: this should be locked
	svc.driver = drivers.m[driver]
	svc.connection, _ = svc.driver.Open(conf)
}

func (s *Service) Close() {
	if s.connection != nil {
		s.connection.Close()
	}
}

// gets called by the driver lib packages
func Register(name string, driver Driver) { svc.Register(name, driver) }
func (svc *Service) Register(name string, driver Driver) {
	// make the driver available in drivers
	drivers.RLock()
	if _, exists := drivers.m[name]; !exists {
		drivers.RUnlock()
		drivers.Lock()
		drivers.m[name] = driver
		drivers.Unlock()
	} else {
		drivers.RUnlock()
	}
}

type ConnParam struct {
	Host   string
	Port   int
	UserId string
	Secret string
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

	// Delete a message from named queue and message identifier
	Delete(queue string, identifier string) (err error)

	// Close the connection
	Close() (err error)
}

// Get a single message from the named queue, blocking until one is received
func GetMessage(name string, noAck bool) (msg Message, err error) { return svc.GetMessage(name, noAck) }
func (svc *Service) GetMessage(name string, noAck bool) (msg Message, err error) {
	return svc.connection.GetOne(name, noAck)
}

// Get a channel from which messages can be read.
func Consume(name string, noAck bool) (c chan Message, err error) {
	return svc.Consume(name, noAck)
}
func (s *Service) Consume(name string, noAck bool) (c chan Message, err error) {
	return s.connection.Consume(name, noAck)
}

// Put a message onto the named queue
func PostMessage(name string, msg Message, delay time.Duration) (err error) {
	return svc.PostMessage(name, msg, delay)
}
func (svc *Service) PostMessage(name string, msg Message, delay time.Duration) (err error) {
	return svc.connection.Post(name, msg, delay)
}
