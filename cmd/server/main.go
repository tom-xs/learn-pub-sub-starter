package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	channel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, &routing.PlayingState{
		IsPaused: true,
	})

	defer channel.Close()
	fmt.Println("Connection sucessfull")

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

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan
	fmt.Println("Execution finished")
	os.Exit(0)
}
