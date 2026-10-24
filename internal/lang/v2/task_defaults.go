package v2

// TaskDefaultsSpec holds default settings that apply to all tasks in a workflow.
// Ported from mistral-master/mistral/lang/v2/task_defaults.py
type TaskDefaultsSpec struct {
	description string
	policies    *PoliciesSpec
	target      interface{}
	keepResult  interface{}
	safeRerun   interface{}
	join        interface{}
	requires    []string
	onComplete  *OnClauseSpec
	onSuccess   *OnClauseSpec
	onError     *OnClauseSpec
	onSkip      *OnClauseSpec
}

func NewTaskDefaultsSpec(data interface{}) *TaskDefaultsSpec {
	if data == nil {
		return nil
	}
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}
	s := &TaskDefaultsSpec{}

	if v, ok := m["description"].(string); ok {
		s.description = v
	}
	if v, ok := m["target"]; ok {
		s.target = v
	}
	if v, ok := m["keep-result"]; ok {
		s.keepResult = v
	}
	if v, ok := m["safe-rerun"]; ok {
		s.safeRerun = v
	}
	if v, ok := m["join"]; ok {
		s.join = v
	}

	// Requires
	if v, ok := m["requires"]; ok {
		switch val := v.(type) {
		case string:
			s.requires = []string{val}
		case []interface{}:
			for _, item := range val {
				if itemStr, ok := item.(string); ok {
					s.requires = append(s.requires, itemStr)
				}
			}
		}
	}

	// Policies
	policyFields := []string{"retry", "wait-before", "wait-after", "timeout", "pause-before", "concurrency", "fail-on"}
	policyMap := make(map[string]interface{})
	for _, f := range policyFields {
		if v, ok := m[f]; ok {
			policyMap[f] = v
		}
	}
	if len(policyMap) > 0 {
		s.policies = NewPoliciesSpec(policyMap)
	}

	// On clauses
	s.onComplete, _ = NewOnClauseSpec(m["on-complete"])
	s.onSuccess, _ = NewOnClauseSpec(m["on-success"])
	s.onError, _ = NewOnClauseSpec(m["on-error"])
	s.onSkip, _ = NewOnClauseSpec(m["on-skip"])

	return s
}

func (s *TaskDefaultsSpec) GetPolicies() *PoliciesSpec   { return s.policies }
func (s *TaskDefaultsSpec) GetTarget() interface{}       { return s.target }
func (s *TaskDefaultsSpec) GetKeepResult() interface{}   { return s.keepResult }
func (s *TaskDefaultsSpec) GetSafeRerun() interface{}    { return s.safeRerun }
func (s *TaskDefaultsSpec) GetJoin() interface{}         { return s.join }
func (s *TaskDefaultsSpec) GetRequires() []string        { return s.requires }
func (s *TaskDefaultsSpec) GetOnComplete() *OnClauseSpec { return s.onComplete }
func (s *TaskDefaultsSpec) GetOnSuccess() *OnClauseSpec  { return s.onSuccess }
func (s *TaskDefaultsSpec) GetOnError() *OnClauseSpec    { return s.onError }
func (s *TaskDefaultsSpec) GetOnSkip() *OnClauseSpec     { return s.onSkip }
