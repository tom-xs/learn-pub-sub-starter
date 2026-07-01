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

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan
	fmt.Println("Execution finished")
	os.Exit(0)
}
