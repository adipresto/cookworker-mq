package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Order is the only wire format. Same id on orders + done for dedupe.
type Order struct {
	ID        string `json:"id"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func dial(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, ch, nil
}

func declare(ch *amqp.Channel, q string) error {
	_, err := ch.QueueDeclare(q, true, false, false, false, nil)
	return err
}

func publish(ch *amqp.Channel, queue string, o Order) error {
	body, err := json.Marshal(o)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, // ponytail: the whole fix for broker-restart loss
		ContentType:  "application/json",
		Body:         body,
	})
}

// runCook: Qos1 manual, process -> publish done -> Ack orders.
func runCook(ch *amqp.Channel, orders, done string) error {
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(orders, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	log.Printf("cook consuming %q (Qos1 manual)", orders)
	for d := range deliveries {
		var o Order
		if err := json.Unmarshal(d.Body, &o); err != nil {
			d.Nack(false, false) // bad message, drop
			continue
		}
		time.Sleep(2 * time.Second) // simulate work
		if err := publish(ch, done, o); err != nil {
			d.Nack(false, true) // done not stored, retry
			continue
		}
		d.Ack(false)
		log.Printf("cooked %s", o.ID)
	}
	return nil
}

// runWaiter: publish one order, then consume done forever (no block on cook).
func runWaiter(ch *amqp.Channel, orders, done string) error {
	value := env("ORDER", "nasi goreng")
	o := Order{
		ID:        fmt.Sprintf("%s-%d", env("HOSTNAME", "waiter"), time.Now().Unix()),
		Value:     value,
		Timestamp: time.Now().Format(time.DateTime),
	}
	if err := publish(ch, orders, o); err != nil {
		return err
	}
	log.Printf("ordered %s %q", o.ID, o.Value)

	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(done, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	seen := map[string]bool{} // ponytail: in-mem dedupe, persistent store if waiter restarts matter
	for d := range deliveries {
		var o Order
		if err := json.Unmarshal(d.Body, &o); err != nil {
			d.Nack(false, false)
			continue
		}
		if seen[o.ID] {
			d.Ack(false)
			continue
		}
		seen[o.ID] = true
		d.Ack(false)
		log.Printf("served %s %q", o.ID, o.Value)
	}
	return nil
}

func main() {
	url := env("AMQP_URL", "amqp://guest:guest@172.28.144.1:5672/")
	orders := env("ORDERS_QUEUE", "orders")
	done := env("DONE_QUEUE", "done")
	role := env("ROLE", "cook")

	conn, ch, err := dial(url)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	defer ch.Close()
	if err := declare(ch, orders); err != nil {
		log.Fatalf("declare %s: %v", orders, err)
	}
	if err := declare(ch, done); err != nil {
		log.Fatalf("declare %s: %v", done, err)
	}

	// SIGTERM (k3s) closes channel: Unacked -> Ready, sibling retries.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		ch.Close()
		conn.Close()
		os.Exit(0)
	}()

	if role == "waiter" {
		log.Fatal(runWaiter(ch, orders, done))
	}
	log.Fatal(runCook(ch, orders, done))
}
