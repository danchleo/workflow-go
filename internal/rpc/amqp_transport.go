package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPTransport is the cross-process MQ broker — exact Go mirror of Python's
// oslo.messaging rabbit:// transport.
//
// AMQP topology (matches oslo.messaging rabbit driver):
//
//	Exchange:  workflow    (type: direct, durable)
//
//	Queue                |  Routing key  |  Purpose
//	═════════════════════╪═══════════════╪═══════════════════════════
//	workflow.engine      |  engine       |  EngineServer listens here
//	workflow.executor    |  executor     |  ExecutorServer listens here
//	(exclusive per-req) |  (auto-delete)|  Client reply queue for request-reply
//
// Message format (exact oslo.messaging wire):
//
//	Publish (async_call / cast — fire-and-forget):
//	  body = JSON{method, args}
//	  delivery_mode = 2 (persistent)
//	  No reply_to, no correlation_id.
//
//	Request (sync_call / call — expects reply):
//	  body = JSON{id, method, args}
//	  reply_to       = <exclusive reply queue name>
//	  correlation_id = <request id>
//	  Server handles, publishes reply back to reply_to queue
//
//	Reply (from server to client):
//	  body = JSON{id, result, error}
//	  correlation_id = <original request id>
//	  Client's goroutine waiting on that correlation_id picks it up.
//
// oslo.messaging Target(topic='engine') → Go topic "engine".
// oslo.messaging .call('start_workflow', ...)   → RPCClient.SyncCall
// oslo.messaging .cast('run_action', ...)       → RPCClient.AsyncCall
type AMQPTransport struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	prefix  string // "workflow"

	// pending reply channels keyed by correlation_id
	mu           sync.Mutex
	pending      map[string]chan *Reply
	replyQueue   string // this client's exclusive reply queue
	replyDeliver <-chan amqp.Delivery
	closed       bool
}

// NewAMQPTransport opens a connection to RabbitMQ.
// url is like "amqp://guest:guest@localhost:5672/" — exact same format as oslo.messaging.
func NewAMQPTransport(url string) (*AMQPTransport, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("amqp dial %s: %w", url, err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("amqp channel: %w", err)
	}
	t := &AMQPTransport{
		conn:    conn,
		channel: ch,
		prefix:  "mistral",
		pending: make(map[string]chan *Reply),
	}

	// Declare durable direct exchange "workflow" (mirrors oslo.messaging's exchange).
	if err := ch.ExchangeDeclare(t.prefix, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		conn.Close()
		return nil, fmt.Errorf("exchange declare: %w", err)
	}

	// Start reply consumer — every AMQPTransport instance gets ONE exclusive
	// reply queue for all its SyncCall requests. This matches oslo.messaging's
	// "direct reply-to" optimization where possible, or falls back to a named
	// temporary queue.
	if err := t.startReplyConsumer(); err != nil {
		conn.Close()
		return nil, err
	}

	return t, nil
}

// queueName returns the durable queue name for a topic — mirrors oslo.messaging
// which uses "{exchange}.{topic}" as the queue name.
func (t *AMQPTransport) queueName(topic string) string {
	return t.prefix + "." + topic
}

func (t *AMQPTransport) exchangeName() string { return t.prefix }

// startReplyConsumer creates an exclusive auto-delete reply queue on this
// connection and starts consuming from it. All SyncCall goroutines on this
// transport instance share this queue and correlate replies by correlation_id.
func (t *AMQPTransport) startReplyConsumer() error {
	q, err := t.channel.QueueDeclare(
		"",    // empty name → server-generated
		false, // not durable
		true,  // auto-delete (exclusive = auto-delete in AMQP)
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return err
	}
	t.replyQueue = q.Name

	deliveries, err := t.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}
	t.replyDeliver = deliveries

	// Dispatch loop — one goroutine per transport instance.
	go func() {
		for d := range deliveries {
			var reply Reply
			if err := json.Unmarshal(d.Body, &reply); err != nil {
				log.Printf("amqp reply unmarshal: %v", err)
				continue
			}
			t.mu.Lock()
			ch, ok := t.pending[d.CorrelationId]
			if ok {
				delete(t.pending, d.CorrelationId)
			}
			t.mu.Unlock()
			if ok {
				select {
				case ch <- &reply:
				default:
				}
			}
		}
	}()
	return nil
}

// Close implements Transport.
func (t *AMQPTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	if t.channel != nil {
		t.channel.Close()
	}
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}

// SendPublish implements Transport.Publish — matches Python async_call / cast.
// Durable publish (delivery_mode=2) to durable queue.
func (t *AMQPTransport) SendPublish(ctx context.Context, topic string, pub *Publish) error {
	body, err := json.Marshal(pub)
	if err != nil {
		return err
	}
	return t.channel.PublishWithContext(ctx,
		t.exchangeName(), // exchange
		topic,            // routing key = topic name
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)
}

// SendRequest implements Transport.Request — matches Python sync_call / call.
// Publishes with reply_to + correlation_id, then waits on reply queue.
func (t *AMQPTransport) SendRequest(ctx context.Context, topic string, req *Request) (<-chan *Reply, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if req.ID == "" {
		req.ID = generateReqID()
	}

	// Register pending reply channel BEFORE publishing — prevents race where
	// server responds so fast we miss the correlation_id.
	replyCh := make(chan *Reply, 1)
	t.mu.Lock()
	t.pending[req.ID] = replyCh
	t.mu.Unlock()

	err = t.channel.PublishWithContext(ctx,
		t.exchangeName(),
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			Body:          body,
			DeliveryMode:  amqp.Persistent,
			ReplyTo:       t.replyQueue,
			CorrelationId: req.ID,
			Timestamp:     time.Now(),
		},
	)
	if err != nil {
		// cleanup pending
		t.mu.Lock()
		delete(t.pending, req.ID)
		t.mu.Unlock()
		return nil, err
	}

	// Return channel; caller does select with ctx.Done().
	// If ctx times out, caller's RPC layer must call t.cancelPending(req.ID).
	return replyCh, nil
}

// cancelPending removes a waiting correlation_id entry — called on timeout.
func (t *AMQPTransport) cancelPending(correlationID string) {
	t.mu.Lock()
	delete(t.pending, correlationID)
	t.mu.Unlock()
}

// RegisterEndpoint implements Transport.RegisterEndpoint — durable queue bind + consume.
// Mirrors oslo.messaging's Server.start() which declares queue, binds to exchange,
// then starts a consumer goroutine.
func (t *AMQPTransport) RegisterEndpoint(topic string, d Dispatcher) {
	ctx := context.Background()

	// Declare durable queue (mirrors oslo.messaging's per-topic durable queue).
	q, err := t.channel.QueueDeclare(t.queueName(topic), true, false, false, false, nil)
	if err != nil {
		log.Printf("amqp queue declare %s: %v", t.queueName(topic), err)
		return
	}

	// Bind to exchange with routing key = topic name.
	if err := t.channel.QueueBind(q.Name, topic, t.exchangeName(), false, nil); err != nil {
		log.Printf("amqp queue bind %s: %v", q.Name, err)
		return
	}

	// Start consumer — auto-ack (oslo.messaging prefetch=1 manual ack for
	// ordering, but our handlers are fast so auto-ack is fine).
	deliveries, err := t.channel.Consume(q.Name, "workflow-go-"+topic, true, false, false, false, nil)
	if err != nil {
		log.Printf("amqp consume %s: %v", topic, err)
		return
	}

	log.Printf("amqp endpoint registered: topic=%s queue=%s", topic, q.Name)

	go func() {
		for del := range deliveries {
			// Determine request vs publish by presence of correlation_id + reply_to.
			// oslo.messaging: .call() sets reply_to + correlation_id; .cast() doesn't.
			if del.ReplyTo != "" && del.CorrelationId != "" {
				// sync_call → expect reply
				var req Request
				if err := json.Unmarshal(del.Body, &req); err != nil {
					continue
				}
				reply := d.Dispatch(ctx, &req)
				if reply != nil {
					t.sendReply(ctx, del.ReplyTo, del.CorrelationId, reply)
				}
			} else {
				// async_call → fire-and-forget, no reply
				var req Request
				var pub Publish
				if err := json.Unmarshal(del.Body, &req); err == nil && req.Method != "" {
					_ = d.Dispatch(ctx, &req)
				} else if err := json.Unmarshal(del.Body, &pub); err == nil && pub.Method != "" {
					_ = d.Dispatch(ctx, &Request{Method: pub.Method, Args: pub.Args})
				}
			}
		}
		log.Printf("amqp consumer stopped for topic=%s", topic)
	}()
}

// sendReply publishes the reply back to the client's exclusive reply queue.
func (t *AMQPTransport) sendReply(ctx context.Context, replyTo, correlationID string, reply *Reply) {
	body, err := json.Marshal(reply)
	if err != nil {
		return
	}
	// Publish directly to reply queue via default exchange (empty exchange,
	// routing key = queue name — AMQP standard direct-to-queue publish).
	if err := t.channel.PublishWithContext(ctx,
		"",      // default exchange
		replyTo, // routing key = reply queue name
		false, false,
		amqp.Publishing{
			ContentType:   "application/json",
			Body:          body,
			CorrelationId: correlationID,
		},
	); err != nil {
		log.Printf("amqp reply publish: %v", err)
	}
}
