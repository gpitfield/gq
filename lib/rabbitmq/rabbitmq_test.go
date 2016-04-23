package rabbitmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/gpitfield/gq"
	"github.com/spf13/viper"
)

const (
	rabbitTestMsg = "hi"
	gqTestMsg     = "hello"
)

func GetTestConfig(t *testing.T) *gq.ConnParam {
	viper.AutomaticEnv()
	host, ok := viper.Get("rabbit_test_host").(string)
	if !ok {
		t.Fatalf("RABBIT_TEST_HOST envvar not set. Please ensure the env var RABBIT_TEST_HOST is set, and points to an accessible rabbit host.")
	}
	port, ok := viper.Get("rabbit_test_port").(int)
	if !ok || port == 0 {
		port = 5672
	}
	user := "guest"
	secret := "guest"
	return &gq.ConnParam{
		Host:   host,
		Port:   port,
		Secret: secret,
		UserId: user,
	}
}

func TestGQ(t *testing.T) {
	params := GetTestConfig(t)
	gq.Open("rabbitmq", params)
	priority := 5
	total := 10
	delay := time.Duration(0)
	for i := 0; i < total; i++ {
		if priority == 5 {
			priority = 1
			delay = time.Duration(2) * time.Second
		} else {
			priority = 5
			delay = time.Duration(0)
		}
		msg := gq.Message{
			Body:     []byte(gqTestMsg),
			Priority: priority,
		}
		fmt.Println("queing priority", msg.Priority)
		err := gq.PostMessage("testing-gq", msg, delay)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}
	rcvd, err := gq.GetMessage("testing-gq", false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if string(rcvd.Body) != gqTestMsg {
		t.Fatalf("%s received instead of %s", string(rcvd.Body), gqTestMsg)
	}
	fmt.Println("Received priority", rcvd.Priority)
	err = rcvd.Ack()
	if err != nil {
		t.Fatalf(err.Error())
	}

	msgChan, err := gq.Consume("testing-gq", false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	i := 0
	fmt.Println("waiting for first message.")
	for message := range msgChan {
		i += 1
		fmt.Println("Received priority", message.Priority)
		fmt.Println("acking msg")
		err = message.Ack()
		if err != nil {
			t.Fatalf(err.Error())
		}
		if i == total-1 {
			break
		}
		fmt.Println("waiting for more...")
	}
}

func TestQueue(t *testing.T) {
	params := GetTestConfig(t)
	priority := 3
	drv := driver{}
	rabbit, err := drv.Open(params)
	defer rabbit.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}
	msg := gq.Message{
		Body:     []byte(rabbitTestMsg),
		Priority: priority,
	}
	batch := 1
	for i := 0; i < batch; i++ {
		err = rabbit.Post("testing", msg, time.Duration(0))
	}
	if err != nil {
		t.Fatalf(err.Error())
	}
	receive, err := rabbit.GetOne("testing", false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if string(receive.Body) != rabbitTestMsg {
		t.Fatalf("%s received instead of %s", string(receive.Body), gqTestMsg)
	}
	if receive.Priority != priority {
		// t.Fatalf("priority changed during message flight.")
	}
	err = receive.Ack()
	if err != nil {
		t.Fatalf(err.Error())
	}
}
