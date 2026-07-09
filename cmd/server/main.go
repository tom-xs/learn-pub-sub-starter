package main

import (
	"fmt"
	"log"

	utils "github.com/bootdotdev/learn-pub-sub-starter/cmd"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	gamelogic.PrintClientHelp()
	conn, err := utils.Connect()
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	fmt.Println("Connection sucessfull")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to create channel: %v", err)
	}

	err = pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.SimpleQueueDurable, handlerLogs())
	if err != nil {
		log.Printf("Unable to subscribe to gob: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		if input[0] == "pause" {
			log.Println("Sending Pause signal")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, &routing.PlayingState{
				IsPaused: true,
			})
			continue
		}
		if input[0] == "resume" {
			log.Println("Sending Resume signal")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, &routing.PlayingState{
				IsPaused: false,
			})
			continue
		}
		if input[0] == "quit" {
			log.Println("Quiting game")
			break
		}
	}

	defer ch.Close()
}

func handlerLogs() func(gamelog routing.GameLog) pubsub.AckType {
	return func(gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
