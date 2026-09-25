package main

import (
	"fmt"
	"os/signal"
	"os"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionUrl := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionUrl)
	if err != nil {
		fmt.Printf("dialing connection url: %s", err)
		return
	}
	defer connection.Close()
	fmt.Println("Connected to RabbitMQ successfully.")

	connChan, err := connection.Channel()
	if err != nil {
		fmt.Printf("openning channel on connection: %s", err)
	}
	pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("welcoming client: %s", err)
	}

	gamestate := gamelogic.NewGameState(username)

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, "transient", handlerPause(gamestate))
	if err != nil {
		fmt.Printf("subscribing to queue: %s", err)
	}


	queueName = fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	routingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, queueName, routingKey, "transient", handlerMove(gamestate, connChan))//maybe need to change channel
	if err != nil {
		fmt.Printf("subscribing to queue: %s", err)
	}

	queueName = "war"
	routingKey = fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, queueName, routingKey, "durable", handlerWarConsumer(gamestate, connChan))
	if err != nil {
		fmt.Printf("subscribing to queue: %s", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			err := gamestate.CommandSpawn(input)
			if err != nil {
				fmt.Printf("incorrect usage of spawn command: %s", err)
			}
		case "move":
			armyMove, err := gamestate.CommandMove(input)
			if err != nil {
				fmt.Printf("incorrect usage of move command: %s", err)
			}

			routingKey := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
			err = pubsub.PublishJSON(connChan, routing.ExchangePerilTopic, routingKey, armyMove)
			if err != nil {
				fmt.Printf("publishing to connection: %s", err)
			}

			fmt.Println("Move was published successfully.")
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) == 1 {
				fmt.Println("To spam you need to provide number of messages: 'spam <number>'")
				continue
			}
			num, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Println("To spam you need to provide number of messages: 'spam <number>'")
			}
			for i := 0; i < num; i++ {
				maliciousMsg := gamelogic.GetMaliciousLog()
				routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)
				err = pubsub.PublishJSON(connChan, routing.ExchangePerilTopic, routingKey, maliciousMsg)
				if err != nil {
					fmt.Printf("publishing to connection: %s", err)
				}	
			}

		case "quit":
			gamelogic.PrintQuit()
		default:
			fmt.Print("incorrect command")
			continue
		}
	}


	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("\nPeril client is shutting down...")
	return
}
