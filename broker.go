package main

import "sync"

// Broker routes published messages to subscribed clients.
type Broker struct {
	mu   sync.RWMutex
	subs map[string]map[*Client]struct{} // topic -> client set
}

func NewBroker() *Broker {
	return &Broker{subs: make(map[string]map[*Client]struct{})}
}

func (b *Broker) Subscribe(topic string, c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs[topic] == nil {
		b.subs[topic] = make(map[*Client]struct{})
	}
	b.subs[topic][c] = struct{}{}
}

func (b *Broker) Unsubscribe(topic string, c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs[topic], c)
	if len(b.subs[topic]) == 0 {
		delete(b.subs, topic)
	}
}

// UnsubscribeAll removes a client from every topic it joined.
// Called on disconnect to prevent the client map from leaking.
func (b *Broker) UnsubscribeAll(c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for topic, clients := range b.subs {
		delete(clients, c)
		if len(clients) == 0 {
			delete(b.subs, topic)
		}
	}
}

// Publish sends msg to all subscribers of topic. Returns subscriber count.
func (b *Broker) Publish(topic, msg string) int {
	b.mu.RLock()
	clients := b.subs[topic]
	b.mu.RUnlock()

	for c := range clients {
		c.send <- "MSG " + topic + " " + msg + "\n"
	}
	return len(clients)
}
