package v2

// RetrySpec represents the retry policy for a task.
// Ported from mistral-master/mistral/lang/v2/retry_policy.py
type RetrySpec struct {
	count   interface{}
	delay   interface{}
	breakOn interface{}
	continueOn interface{}
}

func NewRetrySpec(data interface{}) *RetrySpec {
	if data == nil {
		return nil
	}
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}
	s := &RetrySpec{}
	if v, ok := m["count"]; ok {
		s.count = v
	}
	if v, ok := m["delay"]; ok {
		s.delay = v
	}
	if v, ok := m["break-on"]; ok {
		s.breakOn = v
	}
	if v, ok := m["continue-on"]; ok {
		s.continueOn = v
	}
	return s
}

func (s *RetrySpec) GetCount() interface{}     { return s.count }
func (s *RetrySpec) GetDelay() interface{}     { return s.delay }
func (s *RetrySpec) GetBreakOn() interface{}   { return s.breakOn }
func (s *RetrySpec) GetContinueOn() interface{} { return s.continueOn }
