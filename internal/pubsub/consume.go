package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

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

	isDurable := queueType == "durable"
	isTransient := queueType == "transient"
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
	handler func(T),
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
			handler(body)
			i.Ack(false)
		}
	}()
	return nil
}
