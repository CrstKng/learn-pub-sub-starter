package pubsub

import (
	"fmt"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"context"
	

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

const (
	PrefetchCount = 10
	PrefetchSize = 0
)

type SimpleQueueType string

const (
	QueueTypeDurable SimpleQueueType 	= "durable"
	QueueTypeTransient SimpleQueueType 	= "transient"
)

type AckType string

const (
	Ack AckType 		= "Ack"
	NackRequeue AckType = "NackRequeue"
	NackDiscard AckType = "NackDiscard"
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	newChan, err := conn.Channel()
	if err != nil {
		return newChan, amqp.Queue{}, fmt.Errorf("openning channel on connection: %s", err)
	}

	argsTable := amqp.Table{
		"x-dead-letter-exchange": routing.DeadLetterExchange,
	}

	newQueue, err := newChan.QueueDeclare(queueName, queueType == "durable", queueType == "transient", queueType == "transient", false, argsTable)
	if err != nil {
		return newChan, newQueue, fmt.Errorf("openning queue on channel: %s", err)
	}

	if err := newChan.QueueBind(newQueue.Name, key, exchange, false, nil); err != nil {
		return newChan, newQueue, fmt.Errorf("binding queue to exchange: %s", err)
	}

	return newChan, newQueue, nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	byteVal, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshaling value: %s", err) 
	}

	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: byteVal}); err != nil {
		return fmt.Errorf("publishing value: %s", err) 
	}

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(val)
	if err != nil {
		return fmt.Errorf("gob encoding value: %s", err)
	}

	byteVal := buf.Bytes()
	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/gob", Body: byteVal}); err != nil {
		return fmt.Errorf("publishing value: %s", err) 
	}
	return nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, jsonUmarshal)
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, gobDecode)
}

func subscribe[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType, unmarshaller func([]byte) (T, error)) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("the given queue doesn't exist or is not bound to the exchange: %s", err)
	}

	if err := channel.Qos(PrefetchCount, PrefetchSize, false); err != nil {
		return fmt.Errorf("setting message prefetching conditions with ch.QoS: %s", err)
	}

	deliveryChannels, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("getting delivery channels from ch.Consume: %s", err)
	}

	go func() {
		for deliveryCh := range deliveryChannels {
			var delivery T
			
			delivery, err := unmarshaller(deliveryCh.Body)
			if err != nil {
				fmt.Println("unmarshalling delivery channel: %s", err)
			}

			AckType := handler(delivery)
			switch AckType {
			case Ack:
				fmt.Println("Processed succesfully (Ack).")
				err = deliveryCh.Ack(false)
				if err != nil {
					fmt.Printf("processing ack delivery: %s", err)
				}
			case NackRequeue:
				fmt.Println("Nack and requeue.")
				err = deliveryCh.Nack(false, true)
				if err != nil {
					fmt.Printf("processing nack requeue delivery: %s", err)
				}
			case NackDiscard:
				fmt.Println("Nack and discard.")
				err = deliveryCh.Nack(false, false)
				if err != nil {
					fmt.Printf("processing nack discard delivery: %s", err)
				}
			default:
				fmt.Println("AckType is incorrect!")
			}
		}
		return
	}()
	return nil
}


func jsonUmarshal[T any](byteVal []byte) (T, error) {
	var val T
	if err := json.Unmarshal(byteVal, &val); err != nil {
		return val, err
	}
	return val, nil
}

func gobDecode[T any](byteVal []byte) (T, error) {
	buf := bytes.NewBuffer(byteVal)
	decoder := gob.NewDecoder(buf)
	var val T
	err := decoder.Decode(&val)
	if err != nil {
		return val, err
	}
	return val, nil
	
}