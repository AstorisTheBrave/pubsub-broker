package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// Client represents one connected TCP client.
type Client struct {
	conn   net.Conn
	send   chan string
	broker *Broker
}

func NewClient(conn net.Conn, broker *Broker) *Client {
	return &Client{
		conn:   conn,
		send:   make(chan string, 64),
		broker: broker,
	}
}

// writeLoop drains the send channel and writes to the connection.
// Runs in its own goroutine.
func (c *Client) writeLoop() {
	for msg := range c.send {
		fmt.Fprint(c.conn, msg)
	}
}

// Handle reads commands from the client until it disconnects.
func (c *Client) Handle() {
	defer func() {
		c.broker.UnsubscribeAll(c)
		close(c.send)
		c.conn.Close()
	}()

	go c.writeLoop()
	fmt.Fprint(c.conn, "OK ready\n")

	scanner := bufio.NewScanner(c.conn)
	for scanner.Scan() {
		parts := strings.SplitN(strings.TrimSpace(scanner.Text()), " ", 3)
		if len(parts) == 0 || parts[0] == "" {
			continue
		}

		switch strings.ToUpper(parts[0]) {
		case "SUB":
			if len(parts) < 2 {
				fmt.Fprint(c.conn, "ERR usage: SUB <topic>\n")
				continue
			}
			c.broker.Subscribe(parts[1], c)
			fmt.Fprintf(c.conn, "OK subscribed %s\n", parts[1])

		case "UNSUB":
			if len(parts) < 2 {
				fmt.Fprint(c.conn, "ERR usage: UNSUB <topic>\n")
				continue
			}
			c.broker.Unsubscribe(parts[1], c)
			fmt.Fprintf(c.conn, "OK unsubscribed %s\n", parts[1])

		case "PUB":
			if len(parts) < 3 {
				fmt.Fprint(c.conn, "ERR usage: PUB <topic> <message>\n")
				continue
			}
			n := c.broker.Publish(parts[1], parts[2])
			fmt.Fprintf(c.conn, "OK delivered to %d\n", n)

		case "QUIT":
			fmt.Fprint(c.conn, "OK bye\n")
			return

		default:
			fmt.Fprintf(c.conn, "ERR unknown command %q\n", parts[0])
		}
	}
}
