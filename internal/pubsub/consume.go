package pubsub

import (
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
	queue, err := channel.QueueDeclare(queueName, isDurable, isTransient, isTransient, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = channel.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return channel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	msgs, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for i := range msgs {
			var body T
			if err := json.Unmarshal(i.Body, &body); err != nil {
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
