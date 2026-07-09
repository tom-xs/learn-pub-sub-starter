package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	exchangeKind := "direct"
	if exchange == "peril_topic" {
		exchangeKind = "topic"
	}
	err = channel.ExchangeDeclare(exchange, exchangeKind, true, false, false, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	isDurable := queueType == SimpleQueueDurable
	isTransient := queueType == SimpleQueueTransient
	queue, err := channel.QueueDeclare(queueName, isDurable, isTransient, isTransient, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = channel.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return channel, queue, nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	if err := channel.Qos(10, 0, false); err != nil {
		return err
	}

	msgs, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for i := range msgs {
			body, err := unmarshaller(i.Body)
			if err != nil {
				log.Fatal(err)
			}
			ack := handler(body)
			switch ack {
			case Ack:
				log.Printf("Action for Ack happened: %v", ack)
				i.Ack(false)
			case NackRequeue:
				log.Printf("Action for NackRequeue happened: %v", ack)
				i.Nack(false, true)
			case NackDiscard:
				log.Printf("Action for NackDiscard happened: %v", ack)
				i.Nack(false, false)
			default:
				log.Fatalf("Ack Type non-existent for msg: %v", i)
			}

		}
	}()
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(b []byte) (T, error) {
			buff := bytes.NewBuffer(b)
			dec := gob.NewDecoder(buff)
			var body T
			err := dec.Decode(&body)
			return body, err
		},
	)
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	if err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(b []byte) (T, error) {
			var body T
			err := json.Unmarshal(b, &body)
			return body, err
		},
	); err != nil {
		return err
	}
	return nil
}
