package rabbitmq

import (
	"errors"
	"fmt"
	"log"
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
const RabbitMQConnectionUnavailable = "RabbitMQ host connection unavailable."
const StandardExchange = "gq-immediate"
const DelayExchange = "gq-delay"

func ConnString(params gq.ConnParam) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%v/", params.UserId, params.Secret, params.Host, params.Port)
}

func Queue(c *Channel, name string) (q *amqp.Queue, err error) {
	queues.RLock()
	if _, exists := queues.m[name]; !exists {
		queues.RUnlock()
		queues.Lock()
		var qu amqp.Queue
		ch, err := c.Channel()
		if err != nil {
			return nil, err
		}
		priority := amqp.Table{"x-max-priority": byte(100)}
		qu, err = ch.QueueDeclare(name, true, false, false, false, priority)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		queues.m[name] = &qu
		queues.Unlock()
		err = ch.QueueBind(name, name, c.exchange, false, nil)
		if err != nil {
			return &qu, err
		}
		if c.delayExchange != "" {
			err = ch.QueueBind(name, name, c.delayExchange, false, nil)
		}
	} else {
		queues.RUnlock()
	}
	queues.RLock()
	defer queues.RUnlock()
	return queues.m[name], err
}

func init() {
	gq.Register("rabbitmq", &broker{})
}

type broker struct{}

func (b *broker) Open(params *gq.ConnParam) (conn gq.Connection, err error) {
	rabbitconn, err := amqp.Dial(ConnString(*params))
	if rabbitconn == nil {
		return nil, errors.New(RabbitMQConnectionUnavailable)
	}
	conn = &Channel{
		conn: rabbitconn,
	}
	return
}

type Channel struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	exchange      string
	delayExchange string
}

func (c *Channel) Channel() (ch *amqp.Channel, err error) {
	if c.conn == nil {
		return nil, errors.New(RabbitMQConnectionUnavailable)
	}
	if c.channel == nil {
		c.channel, err = c.conn.Channel()
		if err == nil {
			err = c.channel.ExchangeDeclare(StandardExchange, "direct", true, false, false, false, nil)
			if err == nil {
				c.exchange = StandardExchange
			} else {
				log.Println(err)
				return c.channel, err
			}
			delay := amqp.Table{"x-delayed-type": "direct"}
			err = c.channel.ExchangeDeclare(DelayExchange, "x-delayed-message", true, false, false, false, delay)
			if err == nil {
				c.delayExchange = DelayExchange
			} else {
				log.Println(err)
				return c.channel, err
			}
		}
	}
	return c.channel, err
}

func (c *Channel) Post(queue string, msg gq.Message, delay time.Duration) (err error) {
	ch, err := c.Channel()
	if err != nil {
		return
	}
	if delay != time.Duration(0) && c.delayExchange == "" {
		return errors.New(fmt.Sprintf("%s: %s", UnsupportedCallErr, "this RabbitMQ does not support message delays. Please install and enable the rabbitmq_delayed_message_exchange plugin."))
	}
	var exchange string
	var headers amqp.Table
	if delay > time.Duration(0) {
		headers = amqp.Table{"x-delay": int64(delay / time.Millisecond)} // rabbitmq takes delay in milliseconds vs golang nanoseconds
		log.Printf("delaying this message %v\n", headers)
		exchange = c.delayExchange
	} else {
		exchange = c.exchange
	}
	Queue(c, queue)
	body := msg.Body
	err = ch.Publish(
		exchange, // exchange
		queue,    // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
			Priority:    uint8(msg.Priority),
			Headers:     headers,
		})
	return
}

// create the AckFunc on a copy of the Delivery
func AckFunc(m amqp.Delivery) func() error {
	return func() error {
		log.Printf("Executing ack on GQ msg:%p\n", &m)
		return m.Ack(false)
	}
}

func (c *Channel) Consume(queue string, noAck bool) (out chan gq.Message, err error) {
	ch, err := c.Channel()
	if err != nil {
		return
	}
	ch.Qos(5, 0, true)
	Queue(c, queue)
	msgs, err := ch.Consume(
		queue, // queue
		"",    // consumer
		noAck, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return
	}
	out = make(chan gq.Message)
	go func(messages <-chan amqp.Delivery) {
		for m := range messages {
			var msg gq.Message
			msg.Body = m.Body
			msg.Priority = int(m.Priority)
			if !noAck {
				msg.AckFunc = AckFunc(m)
			}
			out <- msg
		}
	}(msgs)
	return out, err
}

func (c *Channel) GetOne(queue string, noAck bool) (msg gq.Message, err error) {
	ch, err := c.Channel()
	if err != nil {
		return
	}
	Queue(c, queue)
	message, _, err := ch.Get(queue, noAck)
	msg.Body = message.Body
	msg.Priority = int(message.Priority)
	if noAck == false {
		msg.AckFunc = AckFunc(message)
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
