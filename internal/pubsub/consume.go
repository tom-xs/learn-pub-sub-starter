package pubsub

import amqp "github.com/rabbitmq/amqp091-go"

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

	isDurable := queueType == "durable"
	isTransient := queueType == "transient"
	queue, err := channel.QueueDeclare(queueName, isDurable, isTransient, isTransient, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	channel.QueueBind(queue.Name, key, exchange, false, nil)
	return channel, queue, nil
}
