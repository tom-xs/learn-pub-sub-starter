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
	conn, err := utils.Connect()
	if err != nil {
		log.Fatal(err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	_, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, "transient")
	if err != nil {
		log.Fatalf("Unable to subscribe to pause: %v", err)
	}

	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	currentGameState := gamelogic.NewGameState(username)
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			currentGameState.CommandSpawn(input)
		case "move":
			currentGameState.CommandMove(input)
		case "status":
			currentGameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
