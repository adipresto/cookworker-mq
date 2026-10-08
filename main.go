package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
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

// Request is what the TUI sends the waiter. Waiter owns ids.
type Request struct {
	Value string `json:"value"`
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func dial(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

func declare(ch *amqp.Channel, q string) error {
	_, err := ch.QueueDeclare(q, true, false, false, false, nil)
	return err
}

func publish(ch *amqp.Channel, queue string, body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, // ponytail: the whole fix for broker-restart loss
		ContentType:  "application/json",
		Body:         body,
	})
}

func publishOrder(ch *amqp.Channel, queue string, o Order) error {
	body, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return publish(ch, queue, body)
}

// runCook: Qos1 manual, process -> publish done -> Ack orders.
func runCook(conn *amqp.Connection, orders, done string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := declare(ch, orders); err != nil {
		return err
	}
	if err := declare(ch, done); err != nil {
		return err
	}
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
		// ponytail: 15-90s prep so break-during-work is testable, not instant
		prep := time.Duration(15+rand.Intn(76)) * time.Second
		log.Printf("cooking %s %q (%s)", o.ID, o.Value, prep)
		time.Sleep(prep)
		if err := publishOrder(ch, done, o); err != nil {
			d.Nack(false, true) // done not stored, retry
			continue
		}
		d.Ack(false)
		log.Printf("cooked %s", o.ID)
	}
	return nil
}

// runWaiter: turns requests into orders, serves done. Never blocks on cook.
func runWaiter(conn *amqp.Connection, requests, orders, done string) error {
	pub, err := conn.Channel() // own channel: concurrent publish while consuming
	if err != nil {
		return err
	}
	defer pub.Close()
	sub, err := conn.Channel()
	if err != nil {
		return err
	}
	defer sub.Close()
	for _, q := range []string{requests, orders, done} {
		ch := pub
		if q == done {
			ch = sub
		}
		if err := declare(ch, q); err != nil {
			return err
		}
	}

	n := 0
	take := func(value string) string {
		n++
		o := Order{
			ID:        fmt.Sprintf("%s-%d-%d", env("HOSTNAME", "waiter"), time.Now().Unix(), n),
			Value:     value,
			Timestamp: time.Now().Format(time.DateTime),
		}
		if err := publishOrder(pub, orders, o); err != nil {
			log.Printf("order failed: %v", err)
			return ""
		}
		log.Printf("ordered %s %q", o.ID, o.Value)
		return o.ID
	}

	// Waiter pace: one waiter walks every stage serially, like cook prep.
	reg := envDuration("REGISTER_TIME", 3*time.Second)
	hand := envDuration("HANDOFF_TIME", 2*time.Second)
	serve := envDuration("SERVE_TIME", 5*time.Second)
	back := envDuration("RETURN_TIME", 3*time.Second)

	// TUI requests -> orders. Request piles in Ready if waiter is down: no loss.
	reqs, err := sub.Consume(requests, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for d := range reqs {
			var r Request
			if err := json.Unmarshal(d.Body, &r); err != nil || r.Value == "" {
				d.Nack(false, false)
				continue
			}
			log.Printf("registering %q (%s)", r.Value, reg)
			time.Sleep(reg)
			id := take(r.Value)
			if id == "" {
				d.Nack(false, true) // never reached cooks, retry
				continue
			}
			time.Sleep(hand)
			d.Ack(false)
			log.Printf("handed %s to cooks", id)
		}
	}()

	// Optional auto-order (off unless ORDER_EVERY set, e.g. load test).
	if every := envDuration("ORDER_EVERY", 0); every > 0 {
		tick := time.NewTicker(every)
		defer tick.Stop()
		go func() {
			for range tick.C {
				take(env("ORDER", "nasi goreng"))
			}
		}()
	}

	if err := sub.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := sub.Consume(done, "", false, false, false, false, nil)
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
		log.Printf("delivering %s %q to table (%s)", o.ID, o.Value, serve)
		time.Sleep(serve)
		d.Ack(false)
		log.Printf("served %s %q", o.ID, o.Value)
		time.Sleep(back)
		log.Printf("ready at server table")
	}
	return nil
}

func main() {
	url := env("AMQP_URL", "amqp://guest:guest@172.28.144.1:5672/")
	requests := env("REQUESTS_QUEUE", "requests")
	orders := env("ORDERS_QUEUE", "orders")
	done := env("DONE_QUEUE", "done")
	role := env("ROLE", "cook")

	// SIGTERM (k3s) closes the live conn (Unacked -> Ready, sibling retries),
	// then the process dies. Anything else is a broker blip: redial in-process.
	var mu sync.Mutex
	var live *amqp.Connection
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		mu.Lock()
		if live != nil {
			live.Close()
		}
		mu.Unlock()
		os.Exit(0)
	}()

	for {
		c, err := dial(url)
		if err != nil {
			log.Printf("dial: %v (retry in 5s)", err)
			time.Sleep(5 * time.Second)
			continue
		}
		mu.Lock()
		live = c
		mu.Unlock()
		if role == "waiter" {
			err = runWaiter(c, requests, orders, done)
		} else {
			err = runCook(c, orders, done)
		}
		log.Printf("disconnected (%v), redialing...", err)
		c.Close()
		time.Sleep(2 * time.Second)
	}
}
