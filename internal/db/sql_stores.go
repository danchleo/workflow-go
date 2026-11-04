// Package db — gorm/gen-backed SQL CRUD for all 16 Python-parity tables.
//
// Every wrapper type (sqlWfDefStoreSQL, sqlActDefStoreSQL, ...) still exists
// so the Store interface stays stable. All methods now delegate to
// s.store.Q() → gorm/gen query.Query.
//
// Exception: ScheduledJobStore.Capture/GetDueJobs use raw s.store.currentDB()
// because gen lacks IS NULL semantics on nullable string fields.
package db

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danchleo/workflow-go/gen/model"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

func blobMarshal(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// --- WorkflowDefinitionStore ---

type sqlWfDefStoreSQL struct{ store *sqlStore }

func (s *sqlWfDefStoreSQL) Create(wf *WorkflowDefinition) error {
	if wf.ID == "" {
		wf.ID = GenerateID()
	}
	return s.store.Q().WorkflowDefinitionsV2.WithContext(nil).Create(
		&model.WorkflowDefinitionsV2{ID: wf.ID, Name: wf.Name, Data: blobMarshal(wf)},
	)
}

func (s *sqlWfDefStoreSQL) Get(id string) (*WorkflowDefinition, error) {
	q := s.store.Q()
	g, err := q.WorkflowDefinitionsV2.WithContext(nil).Where(q.WorkflowDefinitionsV2.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "WorkflowDefinition", ID: id}
		}
		return nil, err
	}
	var wf WorkflowDefinition
	if err := json.Unmarshal([]byte(g.Data), &wf); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &wf, nil
}

func (s *sqlWfDefStoreSQL) GetByName(name, namespace, projectID string) (*WorkflowDefinition, error) {
	all, err := s.store.Q().WorkflowDefinitionsV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	for _, g := range all {
		var wf WorkflowDefinition
		_ = json.Unmarshal([]byte(g.Data), &wf)
		if wf.Name == name {
			if namespace != "" && wf.Namespace != namespace {
				continue
			}
			if projectID != "" && wf.ProjectID != projectID {
				continue
			}
			return &wf, nil
		}
	}
	return nil, &NotFoundError{Entity: "WorkflowDefinition", ID: name}
}

func (s *sqlWfDefStoreSQL) List(namespace, projectID string) ([]*WorkflowDefinition, error) {
	all, err := s.store.Q().WorkflowDefinitionsV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	out := make([]*WorkflowDefinition, 0, len(all))
	for _, g := range all {
		var wf WorkflowDefinition
		_ = json.Unmarshal([]byte(g.Data), &wf)
		if namespace != "" && wf.Namespace != namespace {
			continue
		}
		if projectID != "" && wf.ProjectID != projectID {
			continue
		}
		out = append(out, &wf)
	}
	return out, nil
}

func (s *sqlWfDefStoreSQL) Update(wf *WorkflowDefinition) error {
	return s.store.Q().WorkflowDefinitionsV2.WithContext(nil).Save(
		&model.WorkflowDefinitionsV2{ID: wf.ID, Name: wf.Name, Data: blobMarshal(wf)},
	)
}

func (s *sqlWfDefStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.WorkflowDefinitionsV2.WithContext(nil).Where(q.WorkflowDefinitionsV2.ID.Eq(id)).Delete()
	return err
}

// --- ActionDefinitionStore ---

type sqlActDefStoreSQL struct{ store *sqlStore }

func (s *sqlActDefStoreSQL) Create(a *ActionDefinition) error {
	if a.ID == "" {
		a.ID = GenerateID()
	}
	return s.store.Q().ActionDefinitionsV2.WithContext(nil).Create(
		&model.ActionDefinitionsV2{ID: a.ID, Name: a.Name, Data: blobMarshal(a)},
	)
}

func (s *sqlActDefStoreSQL) Get(id string) (*ActionDefinition, error) {
	q := s.store.Q()
	g, err := q.ActionDefinitionsV2.WithContext(nil).Where(q.ActionDefinitionsV2.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "ActionDefinition", ID: id}
		}
		return nil, err
	}
	var a ActionDefinition
	if err := json.Unmarshal([]byte(g.Data), &a); err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *sqlActDefStoreSQL) GetByName(name, namespace, projectID string) (*ActionDefinition, error) {
	all, err := s.store.Q().ActionDefinitionsV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	for _, g := range all {
		var a ActionDefinition
		_ = json.Unmarshal([]byte(g.Data), &a)
		if a.Name == name && a.Namespace == namespace && a.ProjectID == projectID {
			return &a, nil
		}
	}
	return nil, &NotFoundError{Entity: "ActionDefinition", ID: name}
}

func (s *sqlActDefStoreSQL) List(namespace, projectID string) ([]*ActionDefinition, error) {
	all, _ := s.store.Q().ActionDefinitionsV2.WithContext(nil).Find()
	out := make([]*ActionDefinition, 0, len(all))
	for _, g := range all {
		var a ActionDefinition
		_ = json.Unmarshal([]byte(g.Data), &a)
		if namespace != "" && a.Namespace != namespace {
			continue
		}
		if projectID != "" && a.ProjectID != projectID {
			continue
		}
		out = append(out, &a)
	}
	return out, nil
}

func (s *sqlActDefStoreSQL) Update(a *ActionDefinition) error {
	return s.store.Q().ActionDefinitionsV2.WithContext(nil).Save(
		&model.ActionDefinitionsV2{ID: a.ID, Name: a.Name, Data: blobMarshal(a)},
	)
}

func (s *sqlActDefStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.ActionDefinitionsV2.WithContext(nil).Where(q.ActionDefinitionsV2.ID.Eq(id)).Delete()
	return err
}

// --- WorkflowExecutionStore ---

type sqlWfExStoreSQL struct{ store *sqlStore }

func (s *sqlWfExStoreSQL) Create(wf *WorkflowExecution) error {
	if wf.ID == "" {
		wf.ID = GenerateID()
	}
	wf.Validate()
	return s.store.Q().WorkflowExecutionsV2.WithContext(nil).Create(
		&model.WorkflowExecutionsV2{ID: wf.ID, WorkflowID: wf.WorkflowID, State: wf.State, Data: blobMarshal(wf)},
	)
}

func (s *sqlWfExStoreSQL) Get(id string) (*WorkflowExecution, error) {
	q := s.store.Q()
	g, err := q.WorkflowExecutionsV2.WithContext(nil).Where(q.WorkflowExecutionsV2.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "WorkflowExecution", ID: id}
		}
		return nil, err
	}
	var wf WorkflowExecution
	if err := json.Unmarshal([]byte(g.Data), &wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

func (s *sqlWfExStoreSQL) List(projectID string) ([]*WorkflowExecution, error) {
	all, _ := s.store.Q().WorkflowExecutionsV2.WithContext(nil).Find()
	out := make([]*WorkflowExecution, 0, len(all))
	for _, g := range all {
		var wf WorkflowExecution
		_ = json.Unmarshal([]byte(g.Data), &wf)
		if projectID != "" && wf.ProjectID != projectID {
			continue
		}
		out = append(out, &wf)
	}
	return out, nil
}

func (s *sqlWfExStoreSQL) Update(wf *WorkflowExecution) error {
	wf.Validate()
	return s.store.Q().WorkflowExecutionsV2.WithContext(nil).Save(
		&model.WorkflowExecutionsV2{ID: wf.ID, WorkflowID: wf.WorkflowID, State: wf.State, Data: blobMarshal(wf)},
	)
}

func (s *sqlWfExStoreSQL) UpdateState(id, curState, newState string) (*WorkflowExecution, error) {
	q := s.store.Q()
	g, err := q.WorkflowExecutionsV2.WithContext(nil).Where(q.WorkflowExecutionsV2.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	var wf WorkflowExecution
	if err := json.Unmarshal([]byte(g.Data), &wf); err != nil {
		return nil, err
	}
	if wf.State != curState {
		return nil, fmt.Errorf("state mismatch: expected %s, got %s", curState, wf.State)
	}
	wf.State = newState
	g.State = newState
	g.Data = blobMarshal(&wf)
	if err := q.WorkflowExecutionsV2.WithContext(nil).Save(g); err != nil {
		return nil, err
	}
	return &wf, nil
}

func (s *sqlWfExStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.WorkflowExecutionsV2.WithContext(nil).Where(q.WorkflowExecutionsV2.ID.Eq(id)).Delete()
	return err
}

// --- TaskExecutionStore ---

type sqlTaskExStoreSQL struct{ store *sqlStore }

func (s *sqlTaskExStoreSQL) Create(t *TaskExecution) error {
	if t.ID == "" {
		t.ID = GenerateID()
	}
	t.Validate()
	return s.store.Q().TaskExecutionsV2.WithContext(nil).Create(&model.TaskExecutionsV2{
		ID: t.ID, WorkflowExecutionID: t.WorkflowExecutionID,
		UniqueKey: t.UniqueKey, State: t.State, Data: blobMarshal(t),
	})
}

func (s *sqlTaskExStoreSQL) Get(id string) (*TaskExecution, error) {
	q := s.store.Q()
	g, err := q.TaskExecutionsV2.WithContext(nil).Where(q.TaskExecutionsV2.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "TaskExecution", ID: id}
		}
		return nil, err
	}
	var t TaskExecution
	if err := json.Unmarshal([]byte(g.Data), &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *sqlTaskExStoreSQL) List(workflowExecutionID, state string) ([]*TaskExecution, error) {
	q := s.store.Q()
	qb := q.TaskExecutionsV2.WithContext(nil)
	if workflowExecutionID != "" {
		qb = qb.Where(q.TaskExecutionsV2.WorkflowExecutionID.Eq(workflowExecutionID))
	}
	if state != "" {
		qb = qb.Where(q.TaskExecutionsV2.State.Eq(state))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*TaskExecution, len(gs))
	for i, g := range gs {
		var t TaskExecution
		_ = json.Unmarshal([]byte(g.Data), &t)
		out[i] = &t
	}
	return out, nil
}

func (s *sqlTaskExStoreSQL) GetByUniqueKey(workflowExecutionID, uniqueKey, state string) ([]*TaskExecution, error) {
	q := s.store.Q()
	qb := q.TaskExecutionsV2.WithContext(nil).Where(
		q.TaskExecutionsV2.WorkflowExecutionID.Eq(workflowExecutionID),
		q.TaskExecutionsV2.UniqueKey.Eq(uniqueKey),
	)
	if state != "" {
		qb = qb.Where(q.TaskExecutionsV2.State.Eq(state))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*TaskExecution, len(gs))
	for i, g := range gs {
		var t TaskExecution
		_ = json.Unmarshal([]byte(g.Data), &t)
		out[i] = &t
	}
	return out, nil
}

func (s *sqlTaskExStoreSQL) Update(t *TaskExecution) error {
	t.Validate()
	return s.store.Q().TaskExecutionsV2.WithContext(nil).Save(&model.TaskExecutionsV2{
		ID: t.ID, WorkflowExecutionID: t.WorkflowExecutionID,
		UniqueKey: t.UniqueKey, State: t.State, Data: blobMarshal(t),
	})
}

func (s *sqlTaskExStoreSQL) UpdateState(id, curState, newState string) (*TaskExecution, error) {
	q := s.store.Q()
	g, err := q.TaskExecutionsV2.WithContext(nil).Where(q.TaskExecutionsV2.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	var t TaskExecution
	if err := json.Unmarshal([]byte(g.Data), &t); err != nil {
		return nil, err
	}
	if t.State != curState {
		return nil, fmt.Errorf("state mismatch: expected %s, got %s", curState, t.State)
	}
	t.State = newState
	g.State = newState
	g.Data = blobMarshal(&t)
	if err := q.TaskExecutionsV2.WithContext(nil).Save(g); err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *sqlTaskExStoreSQL) Count(workflowExecutionID, state string) (int, error) {
	q := s.store.Q()
	qb := q.TaskExecutionsV2.WithContext(nil).Where(q.TaskExecutionsV2.WorkflowExecutionID.Eq(workflowExecutionID))
	if state != "" {
		qb = qb.Where(q.TaskExecutionsV2.State.Eq(state))
	}
	n, err := qb.Count()
	return int(n), err
}

func (s *sqlTaskExStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.TaskExecutionsV2.WithContext(nil).Where(q.TaskExecutionsV2.ID.Eq(id)).Delete()
	return err
}

// --- ActionExecutionStore ---

type sqlActExStoreSQL struct{ store *sqlStore }

func (s *sqlActExStoreSQL) Create(a *ActionExecution) error {
	if a.ID == "" {
		a.ID = GenerateID()
	}
	a.Validate()
	return s.store.Q().ActionExecutionsV2.WithContext(nil).Create(&model.ActionExecutionsV2{
		ID: a.ID, TaskExecutionID: a.TaskExecutionID,
		State: a.State, Data: blobMarshal(a),
	})
}

func (s *sqlActExStoreSQL) Get(id string) (*ActionExecution, error) {
	q := s.store.Q()
	g, err := q.ActionExecutionsV2.WithContext(nil).Where(q.ActionExecutionsV2.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "ActionExecution", ID: id}
		}
		return nil, err
	}
	var a ActionExecution
	if err := json.Unmarshal([]byte(g.Data), &a); err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *sqlActExStoreSQL) List(taskExecutionID, state string) ([]*ActionExecution, error) {
	q := s.store.Q()
	qb := q.ActionExecutionsV2.WithContext(nil)
	if taskExecutionID != "" {
		qb = qb.Where(q.ActionExecutionsV2.TaskExecutionID.Eq(taskExecutionID))
	}
	if state != "" {
		qb = qb.Where(q.ActionExecutionsV2.State.Eq(state))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*ActionExecution, len(gs))
	for i, g := range gs {
		var a ActionExecution
		_ = json.Unmarshal([]byte(g.Data), &a)
		out[i] = &a
	}
	return out, nil
}

func (s *sqlActExStoreSQL) Update(a *ActionExecution) error {
	a.Validate()
	return s.store.Q().ActionExecutionsV2.WithContext(nil).Save(&model.ActionExecutionsV2{
		ID: a.ID, TaskExecutionID: a.TaskExecutionID,
		State: a.State, Data: blobMarshal(a),
	})
}

func (s *sqlActExStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.ActionExecutionsV2.WithContext(nil).Where(q.ActionExecutionsV2.ID.Eq(id)).Delete()
	return err
}

// --- EnvironmentStore ---

type sqlEnvStoreSQL struct{ store *sqlStore }

func (s *sqlEnvStoreSQL) Create(e *Environment) error {
	if e.ID == "" {
		e.ID = GenerateID()
	}
	return s.store.Q().EnvironmentsV2.WithContext(nil).Create(&model.EnvironmentsV2{ID: e.ID, Name: e.Name, Data: blobMarshal(e)})
}

func (s *sqlEnvStoreSQL) Get(id string) (*Environment, error) {
	q := s.store.Q()
	g, err := q.EnvironmentsV2.WithContext(nil).Where(q.EnvironmentsV2.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	var e Environment
	_ = json.Unmarshal([]byte(g.Data), &e)
	return &e, nil
}

func (s *sqlEnvStoreSQL) GetByName(name, projectID string) (*Environment, error) {
	all, _ := s.store.Q().EnvironmentsV2.WithContext(nil).Find()
	for _, g := range all {
		var e Environment
		_ = json.Unmarshal([]byte(g.Data), &e)
		if e.Name == name && (projectID == "" || e.ProjectID == projectID) {
			return &e, nil
		}
	}
	return nil, &NotFoundError{Entity: "Environment", ID: name}
}

func (s *sqlEnvStoreSQL) List(projectID string) ([]*Environment, error) {
	all, _ := s.store.Q().EnvironmentsV2.WithContext(nil).Find()
	out := make([]*Environment, 0, len(all))
	for _, g := range all {
		var e Environment
		_ = json.Unmarshal([]byte(g.Data), &e)
		if projectID != "" && e.ProjectID != projectID {
			continue
		}
		out = append(out, &e)
	}
	return out, nil
}

func (s *sqlEnvStoreSQL) Update(e *Environment) error {
	return s.store.Q().EnvironmentsV2.WithContext(nil).Save(&model.EnvironmentsV2{ID: e.ID, Name: e.Name, Data: blobMarshal(e)})
}

func (s *sqlEnvStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.EnvironmentsV2.WithContext(nil).Where(q.EnvironmentsV2.ID.Eq(id)).Delete()
	return err
}

// --- CronTriggerStore ---

type sqlCronTriggerStore struct{ store *sqlStore }

func (s *sqlCronTriggerStore) Create(ct *CronTrigger) error {
	if ct.ID == "" {
		ct.ID = GenerateID()
	}
	if ct.NextExecutionTime.IsZero() && ct.IntervalSeconds > 0 {
		ct.NextExecutionTime = Now().Add(time.Duration(ct.IntervalSeconds) * time.Second)
	}
	return s.store.Q().CronTriggersV2.WithContext(nil).Create(&model.CronTriggersV2{
		ID: ct.ID, Name: ct.Name,
		NextExecutionTime:   ct.NextExecutionTime.Format(time.RFC3339),
		RemainingExecutions: int64(ct.RemainingExecutions),
		Data:                blobMarshal(ct),
	})
}

func (s *sqlCronTriggerStore) Get(id string) (*CronTrigger, error) {
	q := s.store.Q()
	g, err := q.CronTriggersV2.WithContext(nil).Where(q.CronTriggersV2.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "CronTrigger", ID: id}
		}
		return nil, err
	}
	var ct CronTrigger
	if err := json.Unmarshal([]byte(g.Data), &ct); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &ct, nil
}

func (s *sqlCronTriggerStore) GetByName(name string) (*CronTrigger, error) {
	all, err := s.store.Q().CronTriggersV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	for _, g := range all {
		var ct CronTrigger
		_ = json.Unmarshal([]byte(g.Data), &ct)
		if ct.Name == name {
			return &ct, nil
		}
	}
	return nil, &NotFoundError{Entity: "CronTrigger", ID: name}
}

func (s *sqlCronTriggerStore) List() ([]*CronTrigger, error) {
	all, _ := s.store.Q().CronTriggersV2.WithContext(nil).Find()
	out := make([]*CronTrigger, 0, len(all))
	for _, g := range all {
		var ct CronTrigger
		_ = json.Unmarshal([]byte(g.Data), &ct)
		out = append(out, &ct)
	}
	return out, nil
}

func (s *sqlCronTriggerStore) Update(ct *CronTrigger) error {
	return s.store.Q().CronTriggersV2.WithContext(nil).Save(&model.CronTriggersV2{
		ID: ct.ID, Name: ct.Name,
		NextExecutionTime:   ct.NextExecutionTime.Format(time.RFC3339),
		RemainingExecutions: int64(ct.RemainingExecutions),
		Data:                blobMarshal(ct),
	})
}

func (s *sqlCronTriggerStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.CronTriggersV2.WithContext(nil).Where(q.CronTriggersV2.ID.Eq(id)).Delete()
	return err
}

// --- ScheduledJobStore ---
// gen model's CapturedAt is *string (manually patched) matching MySQL NULL,
// so GetDueJobs can use gen's field.IsNull().
// Capture still uses raw gorm because gen has no conditional UPDATE.

type sqlScheduledJobStore struct{ store *sqlStore }

func (s *sqlScheduledJobStore) Create(j *ScheduledJob) error {
	argsBlob, err := json.Marshal(j.JobArgs)
	if err != nil {
		return err
	}
	now := j.CreatedAt
	if now.IsZero() {
		now = Now()
	}
	var captured *string
	if j.CapturedAt != nil {
		t := j.CapturedAt.Format(time.RFC3339)
		captured = &t
	}
	return s.store.Q().ScheduledJobsV2.WithContext(nil).Create(&model.ScheduledJobsV2{
		ID:         j.ID,
		JobType:    j.JobType,
		JobArgs:    string(argsBlob),
		ExecuteAt:  j.ExecuteAt.Format(time.RFC3339),
		CapturedAt: captured,
		Key:        j.Key,
		CreatedAt:  now.Format(time.RFC3339),
	})
}

// scheduledJobFromGenModel converts gen's model.ScheduledJobsV2 → business ScheduledJob.
// gen's CapturedAt is *string (patched manually) matching MySQL VARCHAR NULL.
func scheduledJobFromGenModel(g *model.ScheduledJobsV2) (*ScheduledJob, error) {
	j := &ScheduledJob{ID: g.ID, JobType: g.JobType, Key: g.Key}
	args := map[string]interface{}{}
	if g.JobArgs != "" {
		if err := json.Unmarshal([]byte(g.JobArgs), &args); err != nil {
			return nil, err
		}
	}
	j.JobArgs = args
	t, err := time.Parse(time.RFC3339, g.ExecuteAt)
	if err != nil {
		return nil, err
	}
	j.ExecuteAt = t
	if g.CapturedAt != nil {
		ct, err := time.Parse(time.RFC3339, *g.CapturedAt)
		if err != nil {
			return nil, err
		}
		j.CapturedAt = &ct
	}
	if g.CreatedAt != "" {
		ct, _ := time.Parse(time.RFC3339, g.CreatedAt)
		j.CreatedAt = ct
	}
	return j, nil
}

func (s *sqlScheduledJobStore) Get(id string) (*ScheduledJob, error) {
	q := s.store.Q()
	g, err := q.ScheduledJobsV2.WithContext(nil).Where(q.ScheduledJobsV2.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	return scheduledJobFromGenModel(g)
}

func (s *sqlScheduledJobStore) List() ([]*ScheduledJob, error) {
	q := s.store.Q()
	gs, err := q.ScheduledJobsV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	out := make([]*ScheduledJob, 0, len(gs))
	for _, g := range gs {
		j, err := scheduledJobFromGenModel(g)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

func (s *sqlScheduledJobStore) GetDueJobs(executeCutoff time.Time, now time.Time, captureTimeout time.Duration, batchSize int) ([]*ScheduledJob, error) {
	q := s.store.Q()
	v2 := q.ScheduledJobsV2
	timeoutBoundary := now.Add(-captureTimeout).Format(time.RFC3339)
	cutoffStr := executeCutoff.Format(time.RFC3339)
	qb := v2.WithContext(nil).Where(
		field.And(
			field.Or(
				v2.CapturedAt.IsNull(),
				v2.CapturedAt.Lt(timeoutBoundary),
			),
			v2.ExecuteAt.Lte(cutoffStr),
		),
	)
	if batchSize > 0 {
		qb = qb.Limit(batchSize)
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*ScheduledJob, 0, len(gs))
	for _, g := range gs {
		j, err := scheduledJobFromGenModel(g)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

func (s *sqlScheduledJobStore) Capture(id string, oldCapturedAt *time.Time, now time.Time) (bool, error) {
	newCaptured := now.Format(time.RFC3339)
	db := s.store.currentDB().WithContext(nil).Model(&model.ScheduledJobsV2{})
	if oldCapturedAt == nil {
		db = db.Where("id = ? AND captured_at IS NULL", id)
	} else {
		oldStr := oldCapturedAt.Format(time.RFC3339)
		db = db.Where("id = ? AND captured_at = ?", id, oldStr)
	}
	result := db.Update("captured_at", newCaptured)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

func (s *sqlScheduledJobStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.ScheduledJobsV2.WithContext(nil).Where(q.ScheduledJobsV2.ID.Eq(id)).Delete()
	return err
}

// --- WorkbookStore ---

type sqlWorkbookStore struct{ store *sqlStore }

func (s *sqlWorkbookStore) Create(wb *Workbook) error {
	if wb.ID == "" {
		wb.ID = GenerateID()
	}
	return s.store.Q().WorkbooksV2.WithContext(nil).Create(&model.WorkbooksV2{ID: wb.ID, Name: wb.Name, Data: blobMarshal(wb)})
}

func (s *sqlWorkbookStore) Get(id string) (*Workbook, error) {
	q := s.store.Q()
	g, err := q.WorkbooksV2.WithContext(nil).Where(q.WorkbooksV2.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	var wb Workbook
	if err := json.Unmarshal([]byte(g.Data), &wb); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &wb, nil
}

func (s *sqlWorkbookStore) GetByName(name, namespace, projectID string) (*Workbook, error) {
	all, err := s.store.Q().WorkbooksV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	for _, g := range all {
		var wb Workbook
		_ = json.Unmarshal([]byte(g.Data), &wb)
		if wb.Name == name {
			if namespace != "" && wb.Namespace != namespace {
				continue
			}
			if projectID != "" && wb.ProjectID != projectID {
				continue
			}
			return &wb, nil
		}
	}
	return nil, &NotFoundError{Entity: "Workbook", ID: name}
}

func (s *sqlWorkbookStore) List(namespace, projectID string) ([]*Workbook, error) {
	all, err := s.store.Q().WorkbooksV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	out := make([]*Workbook, 0, len(all))
	for _, g := range all {
		var wb Workbook
		_ = json.Unmarshal([]byte(g.Data), &wb)
		if namespace != "" && wb.Namespace != namespace {
			continue
		}
		if projectID != "" && wb.ProjectID != projectID {
			continue
		}
		out = append(out, &wb)
	}
	return out, nil
}

func (s *sqlWorkbookStore) Update(wb *Workbook) error {
	return s.store.Q().WorkbooksV2.WithContext(nil).Save(&model.WorkbooksV2{ID: wb.ID, Name: wb.Name, Data: blobMarshal(wb)})
}

func (s *sqlWorkbookStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.WorkbooksV2.WithContext(nil).Where(q.WorkbooksV2.ID.Eq(id)).Delete()
	return err
}

// --- DynamicActionDefinitionStore ---

type sqlDynamicActionDefStore struct{ store *sqlStore }

func (s *sqlDynamicActionDefStore) Create(d *DynamicActionDefinition) error {
	if d.ID == "" {
		d.ID = GenerateID()
	}
	return s.store.Q().DynamicActionDefinition.WithContext(nil).Create(
		&model.DynamicActionDefinition{ID: d.ID, Name: d.Name, Data: blobMarshal(d)},
	)
}

func (s *sqlDynamicActionDefStore) Get(id string) (*DynamicActionDefinition, error) {
	q := s.store.Q()
	g, err := q.DynamicActionDefinition.WithContext(nil).Where(q.DynamicActionDefinition.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	var d DynamicActionDefinition
	if err := json.Unmarshal([]byte(g.Data), &d); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &d, nil
}

func (s *sqlDynamicActionDefStore) GetByName(name, namespace, projectID string) (*DynamicActionDefinition, error) {
	all, err := s.store.Q().DynamicActionDefinition.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	for _, g := range all {
		var d DynamicActionDefinition
		_ = json.Unmarshal([]byte(g.Data), &d)
		if d.Name == name && d.Namespace == namespace && d.ProjectID == projectID {
			return &d, nil
		}
	}
	return nil, &NotFoundError{Entity: "DynamicActionDefinition", ID: name}
}

func (s *sqlDynamicActionDefStore) List(namespace, projectID string) ([]*DynamicActionDefinition, error) {
	all, _ := s.store.Q().DynamicActionDefinition.WithContext(nil).Find()
	out := make([]*DynamicActionDefinition, 0, len(all))
	for _, g := range all {
		var d DynamicActionDefinition
		_ = json.Unmarshal([]byte(g.Data), &d)
		if namespace != "" && d.Namespace != namespace {
			continue
		}
		if projectID != "" && d.ProjectID != projectID {
			continue
		}
		out = append(out, &d)
	}
	return out, nil
}

func (s *sqlDynamicActionDefStore) Update(d *DynamicActionDefinition) error {
	return s.store.Q().DynamicActionDefinition.WithContext(nil).Save(
		&model.DynamicActionDefinition{ID: d.ID, Name: d.Name, Data: blobMarshal(d)},
	)
}

func (s *sqlDynamicActionDefStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.DynamicActionDefinition.WithContext(nil).Where(q.DynamicActionDefinition.ID.Eq(id)).Delete()
	return err
}

// --- EventTriggerStore ---

type sqlEventTriggerStore struct{ store *sqlStore }

func (s *sqlEventTriggerStore) Create(e *EventTrigger) error {
	if e.ID == "" {
		e.ID = GenerateID()
	}
	return s.store.Q().EventTriggersV2.WithContext(nil).Create(&model.EventTriggersV2{ID: e.ID, Name: e.Name, Data: blobMarshal(e)})
}

func (s *sqlEventTriggerStore) Get(id string) (*EventTrigger, error) {
	q := s.store.Q()
	g, err := q.EventTriggersV2.WithContext(nil).Where(q.EventTriggersV2.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	var e EventTrigger
	if err := json.Unmarshal([]byte(g.Data), &e); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &e, nil
}

func (s *sqlEventTriggerStore) GetByExchangeTopicEvent(exchange, topic, event, projectID string) (*EventTrigger, error) {
	all, err := s.store.Q().EventTriggersV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	for _, g := range all {
		var e EventTrigger
		_ = json.Unmarshal([]byte(g.Data), &e)
		if e.Exchange == exchange && e.Topic == topic && e.Event == event {
			if projectID != "" && e.ProjectID != projectID {
				continue
			}
			return &e, nil
		}
	}
	return nil, &NotFoundError{Entity: "EventTrigger", ID: exchange + "/" + topic + "/" + event}
}

func (s *sqlEventTriggerStore) List() ([]*EventTrigger, error) {
	all, _ := s.store.Q().EventTriggersV2.WithContext(nil).Find()
	out := make([]*EventTrigger, 0, len(all))
	for _, g := range all {
		var e EventTrigger
		_ = json.Unmarshal([]byte(g.Data), &e)
		out = append(out, &e)
	}
	return out, nil
}

func (s *sqlEventTriggerStore) Update(e *EventTrigger) error {
	return s.store.Q().EventTriggersV2.WithContext(nil).Save(&model.EventTriggersV2{ID: e.ID, Name: e.Name, Data: blobMarshal(e)})
}

func (s *sqlEventTriggerStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.EventTriggersV2.WithContext(nil).Where(q.EventTriggersV2.ID.Eq(id)).Delete()
	return err
}

// --- ResourceMemberStore ---

type sqlResourceMemberStore struct{ store *sqlStore }

func (s *sqlResourceMemberStore) Create(m *ResourceMember) error {
	if m.ID == "" {
		m.ID = GenerateID()
	}
	return s.store.Q().ResourceMembersV2.WithContext(nil).Create(&model.ResourceMembersV2{ID: m.ID, Data: blobMarshal(m)})
}

func (s *sqlResourceMemberStore) Get(id string) (*ResourceMember, error) {
	q := s.store.Q()
	g, err := q.ResourceMembersV2.WithContext(nil).Where(q.ResourceMembersV2.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	var m ResourceMember
	if err := json.Unmarshal([]byte(g.Data), &m); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &m, nil
}

func (s *sqlResourceMemberStore) ListByResource(resourceID, resourceType string) ([]*ResourceMember, error) {
	all, err := s.store.Q().ResourceMembersV2.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	out := make([]*ResourceMember, 0, len(all))
	for _, g := range all {
		var m ResourceMember
		_ = json.Unmarshal([]byte(g.Data), &m)
		if m.ResourceID == resourceID && m.ResourceType == resourceType {
			out = append(out, &m)
		}
	}
	return out, nil
}

func (s *sqlResourceMemberStore) Update(m *ResourceMember) error {
	return s.store.Q().ResourceMembersV2.WithContext(nil).Save(&model.ResourceMembersV2{ID: m.ID, Data: blobMarshal(m)})
}

func (s *sqlResourceMemberStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.ResourceMembersV2.WithContext(nil).Where(q.ResourceMembersV2.ID.Eq(id)).Delete()
	return err
}

// --- CodeSourceStore ---

type sqlCodeSourceStore struct{ store *sqlStore }

func (s *sqlCodeSourceStore) Create(c *CodeSource) error {
	if c.ID == "" {
		c.ID = GenerateID()
	}
	return s.store.Q().CodeSource.WithContext(nil).Create(&model.CodeSource{ID: c.ID, Name: c.Name, Data: blobMarshal(c)})
}

func (s *sqlCodeSourceStore) Get(id string) (*CodeSource, error) {
	q := s.store.Q()
	g, err := q.CodeSource.WithContext(nil).Where(q.CodeSource.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	var c CodeSource
	if err := json.Unmarshal([]byte(g.Data), &c); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &c, nil
}

func (s *sqlCodeSourceStore) GetByName(name, namespace, projectID string) (*CodeSource, error) {
	all, err := s.store.Q().CodeSource.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	for _, g := range all {
		var c CodeSource
		_ = json.Unmarshal([]byte(g.Data), &c)
		if c.Name == name && c.Namespace == namespace && c.ProjectID == projectID {
			return &c, nil
		}
	}
	return nil, &NotFoundError{Entity: "CodeSource", ID: name}
}

func (s *sqlCodeSourceStore) List(namespace, projectID string) ([]*CodeSource, error) {
	all, _ := s.store.Q().CodeSource.WithContext(nil).Find()
	out := make([]*CodeSource, 0, len(all))
	for _, g := range all {
		var c CodeSource
		_ = json.Unmarshal([]byte(g.Data), &c)
		if namespace != "" && c.Namespace != namespace {
			continue
		}
		if projectID != "" && c.ProjectID != projectID {
			continue
		}
		out = append(out, &c)
	}
	return out, nil
}

func (s *sqlCodeSourceStore) Update(c *CodeSource) error {
	return s.store.Q().CodeSource.WithContext(nil).Save(&model.CodeSource{ID: c.ID, Name: c.Name, Data: blobMarshal(c)})
}

func (s *sqlCodeSourceStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.CodeSource.WithContext(nil).Where(q.CodeSource.ID.Eq(id)).Delete()
	return err
}

// --- MetricsStore ---

type sqlMetricsStore struct{ store *sqlStore }

func (s *sqlMetricsStore) GetByName(name string) (*Metrics, error) {
	q := s.store.Q()
	g, err := q.MistralMetric.WithContext(nil).Where(q.MistralMetric.Name.Eq(name)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "Metrics", ID: name}
		}
		return nil, err
	}
	return &Metrics{Name: g.Name, Value: g.Value}, nil
}

func (s *sqlMetricsStore) Set(name, value string) error {
	q := s.store.Q()
	qb := q.MistralMetric.WithContext(nil).Where(q.MistralMetric.Name.Eq(name))
	if _, err := qb.First(); err == nil {
		return qb.Save(&model.MistralMetric{Name: name, Value: value})
	}
	return q.MistralMetric.WithContext(nil).Create(&model.MistralMetric{Name: name, Value: value})
}

func (s *sqlMetricsStore) Delete(name string) error {
	q := s.store.Q()
	_, err := q.MistralMetric.WithContext(nil).Where(q.MistralMetric.Name.Eq(name)).Delete()
	return err
}
