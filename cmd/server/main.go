package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	pubusb "github.com/bootdotdev/learn-pub-sub-starter/internal"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	pubusb.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, &routing.PlayingState{
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
