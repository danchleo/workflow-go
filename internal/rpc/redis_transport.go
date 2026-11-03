package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisTransport is the cross-process MQ broker — equivalent to Python's
// oslo.messaging rabbit:// transport.
//
// Semantics (1:1 with oslo.messaging):
//
//   SendPublish  (async_call / cast):
//     PUBLISH topic payload   — fire-and-forget, no reply.
//     Matches Python ExecutorClient.run_action(..., async_=True).
//
//   SendRequest  (sync_call / call):
//     1. Client generates request_id, publishes {id, method, args} to topic
//     2. Client calls BLPOP on "reply:{request_id}" (up to timeout)
//     3. Server handles request, RPUSHes {id, result, error} to reply list
//     4. Client pops reply, returns
//     Matches Python EngineClient.start_workflow() which needs the execution back.
//
// Topic convention:
//   workflow:engine    → EngineServer (8 methods)
//   workflow:executor  → ExecutorServer (1 method: run_action)
//
// Test with in-process broker (redis-server --port 6379) or a shared one.
type RedisTransport struct {
	client *redis.Client
	prefix string // default "workflow:"
	mu     sync.Mutex
	subs   map[string]*redis.PubSub // topic → active subscription
	closed bool
}

// NewRedisTransport creates a Redis-backed MQ transport.
// addr is like "localhost:6379" or "redis://user:pass@host:port/db".
func NewRedisTransport(addr string) (*RedisTransport, error) {
	opts, err := redis.ParseURL(addr)
	if err != nil {
		// try plain host:port
		opts = &redis.Options{Addr: addr}
	}
	rdb := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping %s: %w", addr, err)
	}
	return &RedisTransport{
		client: rdb,
		prefix: "workflow:",
		subs:   make(map[string]*redis.PubSub),
	}, nil
}

func (t *RedisTransport) topicName(topic string) string {
	return t.prefix + topic
}

func (t *RedisTransport) replyKey(reqID string) string {
	return t.prefix + "reply:" + reqID
}

func (t *RedisTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for _, sub := range t.subs {
		sub.Close()
	}
	return t.client.Close()
}

// SendPublish implements Transport.Publish — fire-and-forget on Redis pub/sub.
func (t *RedisTransport) SendPublish(ctx context.Context, topic string, pub *Publish) error {
	channel := t.topicName(topic)
	payload, err := json.Marshal(pub)
	if err != nil {
		return err
	}
	return t.client.Publish(ctx, channel, payload).Err()
}

// SendRequest implements Transport.Request — publish + wait on reply list.
func (t *RedisTransport) SendRequest(ctx context.Context, topic string, req *Request) (<-chan *Reply, error) {
	channel := t.topicName(topic)
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	// Publish request.
	if err := t.client.Publish(ctx, channel, payload).Err(); err != nil {
		return nil, err
	}

	// Wait on reply list with BLPOP — timeout matches ctx.
	replyCh := make(chan *Reply, 1)
	go func() {
		key := t.replyKey(req.ID)
		// BLPOP blocks indefinitely — we use a derived context for timeout.
		timeoutCtx := ctx
		if timeoutCtx.Err() != nil {
			// ctx already cancelled
			return
		}
		res, err := t.client.BLPop(timeoutCtx, 0, key).Result()
		if err != nil {
			if err != redis.Nil {
				log.Printf("redis blpop reply: %v", err)
			}
			replyCh <- &Reply{ID: req.ID, Error: err.Error()}
			return
		}
		if len(res) < 2 {
			replyCh <- &Reply{ID: req.ID, Error: "malformed reply list entry"}
			return
		}
		var reply Reply
		if err := json.Unmarshal([]byte(res[1]), &reply); err != nil {
			replyCh <- &Reply{ID: req.ID, Error: "reply unmarshal: " + err.Error()}
			return
		}
		replyCh <- &reply
	}()
	return replyCh, nil
}

// RegisterEndpoint implements Transport.RegisterEndpoint — subscribes to
// topic pub/sub and dispatches messages to Dispatcher.
func (t *RedisTransport) RegisterEndpoint(topic string, d Dispatcher) {
	t.mu.Lock()
	if _, exists := t.subs[topic]; exists {
		t.mu.Unlock()
		return // already subscribed
	}
	t.mu.Unlock()

	channel := t.topicName(topic)
	sub := t.client.Subscribe(context.Background(), channel)
	t.mu.Lock()
	t.subs[topic] = sub
	t.mu.Unlock()

	// Keepalive goroutine — resubscribe on network errors.
	go func() {
		ch := sub.Channel()
		for msg := range ch {
			var raw map[string]any
			if err := json.Unmarshal([]byte(msg.Payload), &raw); err != nil {
				continue
			}
			// Determine request vs publish by presence of "id" field.
			// oslo.messaging: call has msg_id, cast doesn't.
			req := &Request{}
			pub := &Publish{}
			if id, ok := raw["id"]; ok && id != nil {
				// sync_call → expects reply
				if err := json.Unmarshal([]byte(msg.Payload), req); err != nil {
					continue
				}
				reply := d.Dispatch(context.Background(), req)
				t.sendReply(context.Background(), topic, req.ID, reply)
			} else {
				// async_call → fire-and-forget, no reply
				if err := json.Unmarshal([]byte(msg.Payload), pub); err != nil {
					continue
				}
				_ = d.Dispatch(context.Background(), &Request{Method: pub.Method, Args: pub.Args})
			}
		}
		// Channel closed — resubscribe if transport not closed.
		t.mu.Lock()
		closed := t.closed
		t.mu.Unlock()
		if !closed {
			// Resubscribe
			newSub := t.client.Subscribe(context.Background(), channel)
			t.mu.Lock()
			t.subs[topic] = newSub
			t.mu.Unlock()
			// restart loop on new channel
			ch = newSub.Channel()
			for msg := range ch {
				var raw map[string]any
				if err := json.Unmarshal([]byte(msg.Payload), &raw); err != nil {
					continue
				}
				req := &Request{}
				pub := &Publish{}
				if id, ok := raw["id"]; ok && id != nil {
					if err := json.Unmarshal([]byte(msg.Payload), req); err != nil {
						continue
					}
					reply := d.Dispatch(context.Background(), req)
					t.sendReply(context.Background(), topic, req.ID, reply)
				} else {
					if err := json.Unmarshal([]byte(msg.Payload), pub); err != nil {
						continue
					}
					_ = d.Dispatch(context.Background(), &Request{Method: pub.Method, Args: pub.Args})
				}
			}
		}
	}()
}

// sendReply RPUSHes the reply onto the request's reply list so the caller
// (SendRequest goroutine doing BLPOP) can pick it up.
func (t *RedisTransport) sendReply(ctx context.Context, topic, reqID string, reply *Reply) {
	if reply == nil {
		return
	}
	payload, err := json.Marshal(reply)
	if err != nil {
		return
	}
	key := t.replyKey(reqID)
	// RPUSH + EXPIRE (cleanup stale reply lists after 30s)
	if err := t.client.RPush(ctx, key, payload).Err(); err != nil {
		log.Printf("redis rpush reply %s: %v", key, err)
	}
	t.client.Expire(ctx, key, 30*time.Second)
}
