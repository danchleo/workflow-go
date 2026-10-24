// Package notifiers provides a simple webhook / notification interface.
// Ported from mistral-master/mistral/notifiers/
package notifiers

import (
	"fmt"
	"log"
	"time"
)

// NotificationEvent kinds.
const (
	EventWorkflowRunning  = "WORKFLOW_RUNNING"
	EventWorkflowSucceeded = "WORKFLOW_SUCCEEDED"
	EventWorkflowFailed    = "WORKFLOW_FAILED"
	EventWorkflowCancelled = "WORKFLOW_CANCELLED"
	EventWorkflowPaused   = "WORKFLOW_PAUSED"
	EventTaskRunning      = "TASK_RUNNING"
	EventTaskSucceeded    = "TASK_SUCCEEDED"
	EventTaskFailed       = "TASK_FAILED"
	EventTaskCancelled    = "TASK_CANCELLED"
)

// Publisher sends notifications to external subscribers.
type Publisher interface {
	Name() string
	Notify(event string, data map[string]interface{}, target map[string]interface{}) error
}

// NoopPublisher does nothing — useful for testing or when no notifications needed.
type NoopPublisher struct{}

func NewNoopPublisher() *NoopPublisher { return &NoopPublisher{} }

func (p *NoopPublisher) Name() string { return "noop" }
func (p *NoopPublisher) Notify(event string, data map[string]interface{}, target map[string]interface{}) error {
	return nil
}

// WebhookPublisher sends an HTTP POST to a configured URL.
type WebhookPublisher struct {
	Timeout time.Duration
}

func NewWebhookPublisher() *WebhookPublisher {
	return &WebhookPublisher{Timeout: 10 * time.Second}
}

func (p *WebhookPublisher) Name() string { return "webhook" }

func (p *WebhookPublisher) Notify(event string, data map[string]interface{}, target map[string]interface{}) error {
	url, _ := target["url"].(string)
	if url == "" {
		return fmt.Errorf("webhook publisher requires 'url' in target config")
	}
	log.Printf("webhook publisher: would POST %s to %s with data %v", event, url, data)
	return nil // HTTP implementation can be added later.
}

// Manager dispatches notifications to multiple publishers.
type Manager struct {
	publishers []Publisher
}

// NewManager creates a notification manager with the noop publisher.
func NewManager() *Manager {
	return &Manager{
		publishers: []Publisher{NewNoopPublisher()},
	}
}

// AddPublisher adds a publisher.
func (m *Manager) AddPublisher(p Publisher) {
	m.publishers = append(m.publishers, p)
}

// Notify sends an event to all configured publishers.
func (m *Manager) Notify(event string, data map[string]interface{}, targets []map[string]interface{}) {
	for _, t := range targets {
		for _, p := range m.publishers {
			if err := p.Notify(event, data, t); err != nil {
				log.Printf("notifier %s error: %v", p.Name(), err)
			}
		}
	}
}
