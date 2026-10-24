package v2

// PoliciesSpec aggregates all task-level policy settings.
// Ported from mistral-master/mistral/lang/v2/policies.py
type PoliciesSpec struct {
	retry       *RetrySpec
	waitBefore  interface{}
	waitAfter   interface{}
	timeout     interface{}
	pauseBefore interface{}
	concurrency interface{}
	failOn      interface{}
}

func NewPoliciesSpec(data map[string]interface{}) *PoliciesSpec {
	if data == nil {
		return nil
	}
	s := &PoliciesSpec{}
	if v, ok := data["retry"]; ok {
		s.retry = NewRetrySpec(v)
	}
	if v, ok := data["wait-before"]; ok {
		s.waitBefore = v
	}
	if v, ok := data["wait-after"]; ok {
		s.waitAfter = v
	}
	if v, ok := data["timeout"]; ok {
		s.timeout = v
	}
	if v, ok := data["pause-before"]; ok {
		s.pauseBefore = v
	}
	if v, ok := data["concurrency"]; ok {
		s.concurrency = v
	}
	if v, ok := data["fail-on"]; ok {
		s.failOn = v
	}
	return s
}

func (s *PoliciesSpec) GetRetry() *RetrySpec      { return s.retry }
func (s *PoliciesSpec) GetWaitBefore() interface{} { return s.waitBefore }
func (s *PoliciesSpec) GetWaitAfter() interface{}  { return s.waitAfter }
func (s *PoliciesSpec) GetTimeout() interface{}    { return s.timeout }
func (s *PoliciesSpec) GetPauseBefore() interface{} { return s.pauseBefore }
func (s *PoliciesSpec) GetConcurrency() interface{} { return s.concurrency }
func (s *PoliciesSpec) GetFailOn() interface{}     { return s.failOn }
