package rabbitmq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gpitfield/gq"
	"github.com/streadway/amqp"
)

var (
	queues = struct {
		sync.RWMutex
		m map[string]*amqp.Queue
	}{m: make(map[string]*amqp.Queue)}
	rabbitconn *amqp.Connection
)

const UnsupportedCallErr = "Call unimplemented or not supported by RabbitMQ"

func ConnString(params gq.ConnParam) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%v/", params.UserId, params.Secret, params.Host, params.Port)
}

func Queue(ch *amqp.Channel, name string) (q *amqp.Queue, err error) {
	queues.RLock()
	if _, exists := queues.m[name]; !exists {
		queues.RUnlock()
		queues.Lock()
		var qu amqp.Queue
		qu, err = ch.QueueDeclare(name, false, false, false, false, nil)
		queues.m[name] = &qu
		queues.Unlock()
	} else {
		queues.RUnlock()
	}
	queues.RLock()
	defer queues.RUnlock()
	return queues.m[name], err
}

func init() {
	gq.Register("rabbitmq", &driver{})
}

type driver struct{}

func (d *driver) Open(params *gq.ConnParam) (conn gq.Connection, err error) {
	rabbitconn, err := amqp.Dial(ConnString(*params))
	conn = &Channel{
		conn: rabbitconn,
	}
	return
}

type Channel struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func (c *Channel) Channel() (ch *amqp.Channel, err error) {
	if c.channel == nil {
		c.channel, err = c.conn.Channel()
	}
	return c.channel, err
}

func (c *Channel) Post(queue string, msg gq.Message, delay time.Duration) (err error) {
	start := time.Now()
	if delay != time.Duration(0) {
		return errors.New(fmt.Sprintf("%s: %s", UnsupportedCallErr, "rabbitMQ does not support message delays."))
	}
	ch, err := c.Channel()
	if err != nil {
		return
	}
	fmt.Println("since:", time.Since(start))
	Queue(ch, queue)
	body := msg.Body
	err = ch.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
			Priority:    uint8(msg.Priority),
		})
	return
}

func (c *Channel) Consume(queue string, noAck bool) (out chan gq.Message, err error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return
	}
	ch.Qos(1, 0, true)
	Queue(ch, queue)
	msgs, err := ch.Consume(
		queue, // queue
		"",    // consumer
		noAck, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	out = make(chan gq.Message)
	go func(messages <-chan amqp.Delivery) {
		defer ch.Close()
		for m := range messages {
			var msg gq.Message
			msg.Body = m.Body
			msg.Priority = int(m.Priority)
			if !noAck {
				msg.AckFunc = func() error {
					return m.Ack(false)
				}
			}
			out <- msg
		}
	}(msgs)
	return out, err
}

func (c *Channel) GetOne(queue string, noAck bool) (msg gq.Message, err error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return
	}
	Queue(ch, queue)
	message, _, err := ch.Get(queue, noAck)
	msg.Body = message.Body
	msg.Priority = int(message.Priority)
	if noAck == false {
		msg.AckFunc = func() error {
			defer ch.Close()
			return message.Ack(false)
		}
	} else { // only close channel if we don't need it for ack.
		defer ch.Close()
	}
	return
}

func (c *Channel) Delete(queue string, identifier string) (err error) {
	return errors.New(UnsupportedCallErr)
}

func (c *Channel) Close() (err error) {
	if c.channel != nil {
		defer c.channel.Close()
	}
	return c.conn.Close()
}
