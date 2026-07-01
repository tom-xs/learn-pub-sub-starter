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
	conn, err := utils.Connect()
	if err != nil {
		log.Fatal(err)
	}
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, "transient")

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan
	fmt.Println("Execution finished")
	os.Exit(0)
}
