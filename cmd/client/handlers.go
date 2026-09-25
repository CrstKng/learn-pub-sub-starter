package main

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(isPaused routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(isPaused)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		outcome := gs.HandleMove(move)

		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack

		case gamelogic.MoveOutcomeMakeWar:
			routingKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username)
			data := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routingKey, data)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard

		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWarConsumer(gs *gamelogic.GameState, ch *amqp.Channel) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
			case gamelogic.WarOutcomeNotInvolved:
				return pubsub.NackRequeue

			case gamelogic.WarOutcomeNoUnits:
				return pubsub.NackDiscard

			case gamelogic.WarOutcomeYouWon:
				message := fmt.Sprintf("%s won a war against %s\n", winner, loser)
				data := routing.GameLog{
					CurrentTime: time.Now(),
					Message: message,
					Username: gs.Player.Username,
				}
				return PubGameLog(gs, ch, data)

			case gamelogic.WarOutcomeOpponentWon:
				message := fmt.Sprintf("%s won a war against %s\n", winner, loser)
				data := routing.GameLog{
					CurrentTime: time.Now(),
					Message: message,
					Username: gs.Player.Username,
				}
				return PubGameLog(gs, ch, data)

			case gamelogic.WarOutcomeDraw:
				message := fmt.Sprintf("A war between %s and %s resulted in a draw\n", winner, loser)
				data := routing.GameLog{
					CurrentTime: time.Now(),
					Message: message,
					Username: gs.Player.Username,
				}
				return PubGameLog(gs, ch, data)

			default:
				fmt.Printf("Outcome of war is not one of the approved possibilities!")
				return pubsub.NackDiscard
		}
	}
}

func PubGameLog(gs *gamelogic.GameState, ch *amqp.Channel, data routing.GameLog) pubsub.AckType {
	routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, gs.Player.Username)
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routingKey, data)
	if err != nil {
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}