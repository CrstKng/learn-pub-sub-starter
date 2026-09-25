package main

import (
	"fmt"
	"os/signal"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionUrl := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionUrl)
	if err != nil {
		fmt.Printf("dialing connection url: %s", err)
	}
	defer connection.Close()
	fmt.Println("Connected to RabbitMQ successfully.")

	connChan, err := connection.Channel()
	if err != nil {
		fmt.Printf("openning channel on connection: %s", err)
	}
	pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})

	gamelogic.PrintServerHelp()

	queueName := routing.GameLogSlug
	routingKey := fmt.Sprintf("%s.*",routing.GameLogSlug)
	err = pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, queueName, routingKey, "durable", handlerLog())
	if err != nil {
		fmt.Printf("could not subscribe to game logs: %v\n", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			fmt.Print("Sending pause message.")
			pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Print("Sending resume message.")
			pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Print("Exiting.")
			break
		default:
			fmt.Print("incorrect command")
			continue
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("\nPeril server is shutting down...")
	return
}
