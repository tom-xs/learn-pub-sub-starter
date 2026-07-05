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

	channel, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug, pubsub.SimpleQueueDurable)
	if err != nil {
		log.Fatal(err)
	}

	err = channel.ExchangeDeclare(routing.ExchangePerilDirect, "direct", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Connected to queue: %v", queue)

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		if input[0] == "pause" {
			log.Println("Sending Pause signal")
			pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, &routing.PlayingState{
				IsPaused: true,
			})
			continue
		}
		if input[0] == "resume" {
			log.Println("Sending Resume signal")
			pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, &routing.PlayingState{
				IsPaused: false,
			})
			continue
		}
		if input[0] == "quit" {
			log.Println("Quiting game")
			break
		}
	}

	defer channel.Close()
}
