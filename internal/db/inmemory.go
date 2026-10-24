package db

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// InMemoryStore provides an in-memory implementation of the Store interface.
// Suitable for development, testing, and small deployments.
type InMemoryStore struct {
	mu sync.RWMutex

	workflowDefs map[string]*WorkflowDefinition
	actionDefs   map[string]*ActionDefinition
	wfExecs      map[string]*WorkflowExecution
	taskExecs    map[string]*TaskExecution
	actionExecs  map[string]*ActionExecution
	environments map[string]*Environment
	cronTriggers map[string]*CronTrigger

	// Locks: map of lock name -> channel used for simple mutex semantics.
	locks map[string]chan struct{}
}

// NewInMemoryStore creates a new in-memory store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		workflowDefs: make(map[string]*WorkflowDefinition),
		actionDefs:   make(map[string]*ActionDefinition),
		wfExecs:      make(map[string]*WorkflowExecution),
		taskExecs:    make(map[string]*TaskExecution),
		actionExecs:  make(map[string]*ActionExecution),
		environments: make(map[string]*Environment),
		cronTriggers: make(map[string]*CronTrigger),
		locks:        make(map[string]chan struct{}),
	}
}

func (s *InMemoryStore) WorkflowDefinitions() WorkflowDefinitionStore { return &wmDefStore{s} }
func (s *InMemoryStore) ActionDefinitions() ActionDefinitionStore     { return &amDefStore{s} }
func (s *InMemoryStore) WorkflowExecutions() WorkflowExecutionStore   { return &wmExecStore{s} }
func (s *InMemoryStore) TaskExecutions() TaskExecutionStore           { return &tmExecStore{s} }
func (s *InMemoryStore) ActionExecutions() ActionExecutionStore       { return &amExecStore{s} }
func (s *InMemoryStore) Environments() EnvironmentStore               { return &envStore{s} }
func (s *InMemoryStore) CronTriggers() CronTriggerStore               { return &memCronTriggerStore{s} }

func (s *InMemoryStore) AcquireLock(name string) error {
	s.mu.Lock()
	ch, ok := s.locks[name]
	if !ok {
		ch = make(chan struct{}, 1)
		s.locks[name] = ch
	}
	s.mu.Unlock()
	ch <- struct{}{}
	return nil
}

func (s *InMemoryStore) ReleaseLock(name string) error {
	s.mu.RLock()
	ch, ok := s.locks[name]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("lock %s not held", name)
	}
	<-ch
	return nil
}

func (s *InMemoryStore) RunInTransaction(fn func(Store) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(s)
}

// --- WorkflowDefinitionStore ---

type wmDefStore struct{ s *InMemoryStore }

func (w *wmDefStore) Create(wf *WorkflowDefinition) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	wf.ID = GenerateID()
	now := Now()
	wf.CreatedAt = now
	wf.UpdatedAt = now
	w.s.workflowDefs[wf.ID] = wf
	return nil
}

func (w *wmDefStore) Get(id string) (*WorkflowDefinition, error) {
	w.s.mu.RLock()
	defer w.s.mu.RUnlock()
	wf, ok := w.s.workflowDefs[id]
	if !ok {
		return nil, &NotFoundError{Entity: "WorkflowDefinition", ID: id}
	}
	return wf, nil
}

func (w *wmDefStore) GetByName(name, namespace, projectID string) (*WorkflowDefinition, error) {
	w.s.mu.RLock()
	defer w.s.mu.RUnlock()
	for _, wf := range w.s.workflowDefs {
		if wf.Name == name && wf.Namespace == namespace && wf.ProjectID == projectID {
			return wf, nil
		}
	}
	return nil, &NotFoundError{Entity: "WorkflowDefinition", ID: name}
}

func (w *wmDefStore) List(namespace, projectID string) ([]*WorkflowDefinition, error) {
	w.s.mu.RLock()
	defer w.s.mu.RUnlock()
	var result []*WorkflowDefinition
	for _, wf := range w.s.workflowDefs {
		if namespace != "" && wf.Namespace != namespace {
			continue
		}
		if projectID != "" && wf.ProjectID != projectID {
			continue
		}
		result = append(result, wf)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (w *wmDefStore) Update(wf *WorkflowDefinition) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	existing, ok := w.s.workflowDefs[wf.ID]
	if !ok {
		return &NotFoundError{Entity: "WorkflowDefinition", ID: wf.ID}
	}
	wf.CreatedAt = existing.CreatedAt
	wf.UpdatedAt = Now()
	w.s.workflowDefs[wf.ID] = wf
	return nil
}

func (w *wmDefStore) Delete(id string) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	delete(w.s.workflowDefs, id)
	return nil
}

// --- ActionDefinitionStore ---

type amDefStore struct{ s *InMemoryStore }

func (a *amDefStore) Create(ad *ActionDefinition) error {
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	ad.ID = GenerateID()
	now := Now()
	ad.CreatedAt = now
	ad.UpdatedAt = now
	a.s.actionDefs[ad.ID] = ad
	return nil
}

func (a *amDefStore) Get(id string) (*ActionDefinition, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	ad, ok := a.s.actionDefs[id]
	if !ok {
		return nil, &NotFoundError{Entity: "ActionDefinition", ID: id}
	}
	return ad, nil
}

func (a *amDefStore) GetByName(name, namespace, projectID string) (*ActionDefinition, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	for _, ad := range a.s.actionDefs {
		if ad.Name == name && ad.Namespace == namespace && ad.ProjectID == projectID {
			return ad, nil
		}
	}
	return nil, &NotFoundError{Entity: "ActionDefinition", ID: name}
}

func (a *amDefStore) List(namespace, projectID string) ([]*ActionDefinition, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	var result []*ActionDefinition
	for _, ad := range a.s.actionDefs {
		if namespace != "" && ad.Namespace != namespace {
			continue
		}
		if projectID != "" && ad.ProjectID != projectID {
			continue
		}
		result = append(result, ad)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (a *amDefStore) Update(ad *ActionDefinition) error {
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	a.s.actionDefs[ad.ID] = ad
	return nil
}

func (a *amDefStore) Delete(id string) error {
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	delete(a.s.actionDefs, id)
	return nil
}

// --- WorkflowExecutionStore ---

type wmExecStore struct{ s *InMemoryStore }

func (w *wmExecStore) Create(wf *WorkflowExecution) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if wf.ID == "" {
		wf.ID = GenerateID()
	}
	now := Now()
	wf.CreatedAt = now
	wf.UpdatedAt = now
	if wf.Input == nil {
		wf.Input = make(map[string]interface{})
	}
	if wf.Output == nil {
		wf.Output = make(map[string]interface{})
	}
	if wf.Params == nil {
		wf.Params = make(map[string]interface{})
	}
	if wf.Context == nil {
		wf.Context = make(map[string]interface{})
	}
	if wf.RuntimeContext == nil {
		wf.RuntimeContext = make(map[string]interface{})
	}
	w.s.wfExecs[wf.ID] = wf
	return nil
}

func (w *wmExecStore) Get(id string) (*WorkflowExecution, error) {
	w.s.mu.RLock()
	defer w.s.mu.RUnlock()
	wf, ok := w.s.wfExecs[id]
	if !ok {
		return nil, &NotFoundError{Entity: "WorkflowExecution", ID: id}
	}
	return wf, nil
}

func (w *wmExecStore) List(projectID string) ([]*WorkflowExecution, error) {
	w.s.mu.RLock()
	defer w.s.mu.RUnlock()
	var result []*WorkflowExecution
	for _, wf := range w.s.wfExecs {
		if projectID != "" && wf.ProjectID != projectID {
			continue
		}
		result = append(result, wf)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (w *wmExecStore) Update(wf *WorkflowExecution) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	existing, ok := w.s.wfExecs[wf.ID]
	if !ok {
		return &NotFoundError{Entity: "WorkflowExecution", ID: wf.ID}
	}
	wf.CreatedAt = existing.CreatedAt
	wf.UpdatedAt = Now()
	w.s.wfExecs[wf.ID] = wf
	return nil
}

func (w *wmExecStore) UpdateState(id string, curState, newState string) (*WorkflowExecution, error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	wf, ok := w.s.wfExecs[id]
	if !ok {
		return nil, &NotFoundError{Entity: "WorkflowExecution", ID: id}
	}
	if wf.State != curState {
		return nil, nil // Optimistic lock failed
	}
	wf.State = newState
	wf.UpdatedAt = Now()
	return wf, nil
}

func (w *wmExecStore) Delete(id string) error {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	delete(w.s.wfExecs, id)
	return nil
}

// --- TaskExecutionStore ---

type tmExecStore struct{ s *InMemoryStore }

func (t *tmExecStore) Create(te *TaskExecution) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	if te.ID == "" {
		te.ID = GenerateID()
	}
	now := Now()
	te.CreatedAt = now
	te.UpdatedAt = now
	if te.InContext == nil {
		te.InContext = make(map[string]interface{})
	}
	if te.Published == nil {
		te.Published = make(map[string]interface{})
	}
	if te.RuntimeContext == nil {
		te.RuntimeContext = make(map[string]interface{})
	}
	t.s.taskExecs[te.ID] = te
	return nil
}

func (t *tmExecStore) Get(id string) (*TaskExecution, error) {
	t.s.mu.RLock()
	defer t.s.mu.RUnlock()
	te, ok := t.s.taskExecs[id]
	if !ok {
		return nil, &NotFoundError{Entity: "TaskExecution", ID: id}
	}
	t.loadChildren(te)
	return te, nil
}

func (t *tmExecStore) loadChildren(te *TaskExecution) {
	te.ActionExecutions = nil
	te.WorkflowExecutions = nil

	for _, ae := range t.s.actionExecs {
		if ae.TaskExecutionID == te.ID {
			te.ActionExecutions = append(te.ActionExecutions, ae)
		}
	}
	for _, wf := range t.s.wfExecs {
		if wf.TaskExecutionID == te.ID {
			te.WorkflowExecutions = append(te.WorkflowExecutions, wf)
		}
	}
}

func (t *tmExecStore) List(workflowExecutionID, state string) ([]*TaskExecution, error) {
	t.s.mu.RLock()
	defer t.s.mu.RUnlock()

	var result []*TaskExecution
	for _, te := range t.s.taskExecs {
		if workflowExecutionID != "" && te.WorkflowExecutionID != workflowExecutionID {
			continue
		}
		if state != "" && te.State != state {
			continue
		}
		result = append(result, te)
	}

	// Sort by name.
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	// Load children.
	for _, te := range result {
		t.loadChildren(te)
	}

	return result, nil
}

func (t *tmExecStore) GetByUniqueKey(workflowExecutionID, uniqueKey, state string) ([]*TaskExecution, error) {
	t.s.mu.RLock()
	defer t.s.mu.RUnlock()

	var result []*TaskExecution
	for _, te := range t.s.taskExecs {
		if workflowExecutionID != "" && te.WorkflowExecutionID != workflowExecutionID {
			continue
		}
		if uniqueKey != "" && te.UniqueKey != uniqueKey {
			continue
		}
		if state != "" && te.State != state {
			continue
		}
		result = append(result, te)
	}
	for _, te := range result {
		t.loadChildren(te)
	}
	return result, nil
}

func (t *tmExecStore) Update(te *TaskExecution) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	existing, ok := t.s.taskExecs[te.ID]
	if !ok {
		return &NotFoundError{Entity: "TaskExecution", ID: te.ID}
	}
	te.CreatedAt = existing.CreatedAt
	te.UpdatedAt = Now()
	t.s.taskExecs[te.ID] = te
	return nil
}

func (t *tmExecStore) UpdateState(id string, curState, newState string) (*TaskExecution, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	te, ok := t.s.taskExecs[id]
	if !ok {
		return nil, &NotFoundError{Entity: "TaskExecution", ID: id}
	}
	if te.State != curState {
		return nil, nil // Optimistic lock failed
	}
	te.State = newState
	te.UpdatedAt = Now()
	return te, nil
}

func (t *tmExecStore) Count(workflowExecutionID, state string) (int, error) {
	t.s.mu.RLock()
	defer t.s.mu.RUnlock()

	count := 0
	for _, te := range t.s.taskExecs {
		if workflowExecutionID != "" && te.WorkflowExecutionID != workflowExecutionID {
			continue
		}
		if state != "" && te.State != state {
			continue
		}
		count++
	}
	return count, nil
}

func (t *tmExecStore) Delete(id string) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	delete(t.s.taskExecs, id)
	return nil
}

// --- ActionExecutionStore ---

type amExecStore struct{ s *InMemoryStore }

func (a *amExecStore) Create(ae *ActionExecution) error {
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if ae.ID == "" {
		ae.ID = GenerateID()
	}
	now := Now()
	ae.CreatedAt = now
	ae.UpdatedAt = now
	if ae.Input == nil {
		ae.Input = make(map[string]interface{})
	}
	if ae.Output == nil {
		ae.Output = make(map[string]interface{})
	}
	if ae.RuntimeContext == nil {
		ae.RuntimeContext = make(map[string]interface{})
	}
	a.s.actionExecs[ae.ID] = ae
	return nil
}

func (a *amExecStore) Get(id string) (*ActionExecution, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	ae, ok := a.s.actionExecs[id]
	if !ok {
		return nil, &NotFoundError{Entity: "ActionExecution", ID: id}
	}
	return ae, nil
}

func (a *amExecStore) List(taskExecutionID, state string) ([]*ActionExecution, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	var result []*ActionExecution
	for _, ae := range a.s.actionExecs {
		if taskExecutionID != "" && ae.TaskExecutionID != taskExecutionID {
			continue
		}
		if state != "" && ae.State != state {
			continue
		}
		result = append(result, ae)
	}
	return result, nil
}

func (a *amExecStore) Update(ae *ActionExecution) error {
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	existing, ok := a.s.actionExecs[ae.ID]
	if !ok {
		return &NotFoundError{Entity: "ActionExecution", ID: ae.ID}
	}
	ae.CreatedAt = existing.CreatedAt
	ae.UpdatedAt = Now()
	a.s.actionExecs[ae.ID] = ae
	return nil
}

func (a *amExecStore) Delete(id string) error {
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	delete(a.s.actionExecs, id)
	return nil
}

// --- EnvironmentStore ---

type envStore struct{ s *InMemoryStore }

func (e *envStore) Create(env *Environment) error {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	env.ID = GenerateID()
	now := Now()
	env.CreatedAt = now
	env.UpdatedAt = now
	if env.Variables == nil {
		env.Variables = make(map[string]interface{})
	}
	e.s.environments[env.ID] = env
	return nil
}

func (e *envStore) Get(id string) (*Environment, error) {
	e.s.mu.RLock()
	defer e.s.mu.RUnlock()
	env, ok := e.s.environments[id]
	if !ok {
		return nil, &NotFoundError{Entity: "Environment", ID: id}
	}
	return env, nil
}

func (e *envStore) GetByName(name, projectID string) (*Environment, error) {
	e.s.mu.RLock()
	defer e.s.mu.RUnlock()
	for _, env := range e.s.environments {
		if env.Name == name && env.ProjectID == projectID {
			return env, nil
		}
	}
	return nil, &NotFoundError{Entity: "Environment", ID: name}
}

func (e *envStore) List(projectID string) ([]*Environment, error) {
	e.s.mu.RLock()
	defer e.s.mu.RUnlock()
	var result []*Environment
	for _, env := range e.s.environments {
		if projectID != "" && env.ProjectID != projectID {
			continue
		}
		result = append(result, env)
	}
	return result, nil
}

func (e *envStore) Update(env *Environment) error {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	e.s.environments[env.ID] = env
	return nil
}

func (e *envStore) Delete(id string) error {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	delete(e.s.environments, id)
	return nil
}

// --- CronTriggerStore ---

type memCronTriggerStore struct{ s *InMemoryStore }

func (m *memCronTriggerStore) Create(ct *CronTrigger) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if ct.ID == "" {
		ct.ID = GenerateID()
	}
	if ct.NextExecutionTime.IsZero() && ct.IntervalSeconds > 0 {
		ct.NextExecutionTime = Now().Add(time.Duration(ct.IntervalSeconds) * time.Second)
	}
	now := Now()
	ct.CreatedAt = now
	ct.UpdatedAt = now
	m.s.cronTriggers[ct.ID] = ct
	return nil
}

func (m *memCronTriggerStore) Get(id string) (*CronTrigger, error) {
	m.s.mu.RLock()
	defer m.s.mu.RUnlock()
	ct, ok := m.s.cronTriggers[id]
	if !ok {
		return nil, &NotFoundError{Entity: "CronTrigger", ID: id}
	}
	return ct, nil
}

func (m *memCronTriggerStore) GetByName(name string) (*CronTrigger, error) {
	m.s.mu.RLock()
	defer m.s.mu.RUnlock()
	for _, ct := range m.s.cronTriggers {
		if ct.Name == name {
			return ct, nil
		}
	}
	return nil, &NotFoundError{Entity: "CronTrigger", ID: name}
}

func (m *memCronTriggerStore) List() ([]*CronTrigger, error) {
	m.s.mu.RLock()
	defer m.s.mu.RUnlock()
	result := make([]*CronTrigger, 0, len(m.s.cronTriggers))
	for _, ct := range m.s.cronTriggers {
		result = append(result, ct)
	}
	return result, nil
}

func (m *memCronTriggerStore) Update(ct *CronTrigger) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	existing, ok := m.s.cronTriggers[ct.ID]
	if !ok {
		return &NotFoundError{Entity: "CronTrigger", ID: ct.ID}
	}
	ct.CreatedAt = existing.CreatedAt
	ct.UpdatedAt = Now()
	m.s.cronTriggers[ct.ID] = ct
	return nil
}

func (m *memCronTriggerStore) Delete(id string) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	delete(m.s.cronTriggers, id)
	return nil
}

// Helper: check whether a string starts with a given prefix (for filtering).
func hasPrefix(s, prefix string) bool { return strings.HasPrefix(s, prefix) }
