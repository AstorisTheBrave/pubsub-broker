# pubsub-broker

A TCP pub/sub message broker. Clients connect over a plain text protocol,
subscribe to topics, and publish messages. Subscribers receive matching
messages in real time.

## run

```bash
go run .
go run . 9000   # custom port
```

## protocol

Commands are newline-terminated plain text.

```
SUB <topic>           subscribe to a topic
UNSUB <topic>         unsubscribe
PUB <topic> <msg>     publish message to all subscribers of topic
QUIT                  disconnect
```

Responses start with `OK` or `ERR`.

Delivered messages arrive as `MSG <topic> <message>`.

## example (two terminal windows)

**terminal 1 - subscriber:**
```bash
nc localhost 9000
SUB events
# OK subscribed events
# MSG events hello from terminal 2
```

**terminal 2 - publisher:**
```bash
nc localhost 9000
PUB events hello from terminal 2
# OK delivered to 1
```

## what this shows

- one goroutine per client for reads, one for writes (no blocking I/O on the broker)
- `sync.RWMutex` separating read-heavy subscription lookups from write-path mutations
- buffered send channel (size 64) so a slow subscriber can't block a publisher
- clean disconnect: `UnsubscribeAll` + `close(send)` + `conn.Close()` in a single `defer`
- plain text protocol - testable with just `nc`
