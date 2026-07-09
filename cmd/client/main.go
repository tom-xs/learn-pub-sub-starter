package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	utils "github.com/bootdotdev/learn-pub-sub-starter/cmd"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := utils.Connect()
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Couldn't create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Cloud not get username: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gs.GetUsername(),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("Unable to subscribe to pause: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(gs, ch),
	)
	if err != nil {
		log.Fatalf("Unable to subscribe to war: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gs.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gs, ch),
	)
	if err != nil {
		log.Fatalf("Unable to subscribe to moves: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			err := gs.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+gs.GetUsername(), move)
			if err != nil {
				fmt.Printf("error: %v", err)
				continue
			}

		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) < 2 {
				fmt.Println("Second parameter missing")
				continue
			}
			times, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Printf("Unable to convert 2nd input to integer: %v", err)
				continue
			}
			for {
				if times <= 0 {
					break
				}
				msg := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGob(
					ch,
					routing.ExchangePerilTopic,
					routing.GameLogSlug+"."+gs.GetUsername(),
					routing.GameLog{
						Username:    gs.GetUsername(),
						CurrentTime: time.Now(),
						Message:     msg,
					},
				)
				if err != nil {
					fmt.Printf("Error while publishing gob: %v", err)
				}
				times--
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rec gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(rec)
		key := fmt.Sprintf("%s.%s", routing.GameLogSlug, rec.Attacker.Username)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			log.Printf("war outcome: %v winner=%s loser=%s attacker=%s", warOutcome, winner, loser, rec.Attacker.Username)
			if err := publishWarGob(ch, routing.ExchangePerilTopic, key, rec.Attacker.Username, msg); err != nil {
				log.Printf("Unable to publish gob: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			log.Printf("war outcome: %v winner=%s loser=%s attacker=%s", warOutcome, winner, loser, rec.Attacker.Username)
			if err := publishWarGob(ch, routing.ExchangePerilTopic, key, rec.Attacker.Username, msg); err != nil {
				log.Printf("Unable to publish gob: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			log.Printf("WarOutcome %v not available", warOutcome)
			return pubsub.NackDiscard
		}
	}
}

func publishWarGob(ch *amqp.Channel, topic, key, username, message string) error {
	gl := routing.GameLog{
		CurrentTime: time.Now(),
		Username:    username,
		Message:     message,
	}
	return pubsub.PublishGob(ch, topic, key, gl)
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		mo := gs.HandleMove(am)
		switch mo {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			routingKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername())
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routingKey, gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				log.Printf("Unable to publish MoveOutcome: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}
