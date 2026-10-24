package db

import (
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Each GORM model stores the complete db model as a JSON blob in TEXT.
// This avoids hand-mapping every field — the db structs already carry
// all the data we need; we just serialize them for SQLite.

func blobMarshal(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// --- WorkflowDefinitionStore ---

type gWorkflowDef struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"index"`
	Data string // JSON blob of *WorkflowDefinition
}

func (gWorkflowDef) TableName() string { return "workflow_definitions" }

type sqlWfDefStoreSQL struct{ db *gorm.DB }

func (s *sqlWfDefStoreSQL) Create(wf *WorkflowDefinition) error {
	if wf.ID == "" {
		wf.ID = GenerateID()
	}
	return s.db.Create(&gWorkflowDef{ID: wf.ID, Name: wf.Name, Data: blobMarshal(wf)}).Error
}
func (s *sqlWfDefStoreSQL) Get(id string) (*WorkflowDefinition, error) {
	var g gWorkflowDef
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
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
	var all []gWorkflowDef
	if err := s.db.Find(&all).Error; err != nil {
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
	var all []gWorkflowDef
	if err := s.db.Find(&all).Error; err != nil {
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
	return s.db.Save(&gWorkflowDef{ID: wf.ID, Name: wf.Name, Data: blobMarshal(wf)}).Error
}
func (s *sqlWfDefStoreSQL) Delete(id string) error {
	return s.db.Delete(&gWorkflowDef{}, "id = ?", id).Error
}

// --- ActionDefinitionStore (same blob pattern) ---

type gActionDef struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"index"`
	Data string
}

func (gActionDef) TableName() string { return "action_definitions" }

type sqlActDefStoreSQL struct{ db *gorm.DB }

func (s *sqlActDefStoreSQL) Create(a *ActionDefinition) error {
	if a.ID == "" {
		a.ID = GenerateID()
	}
	return s.db.Create(&gActionDef{ID: a.ID, Name: a.Name, Data: blobMarshal(a)}).Error
}
func (s *sqlActDefStoreSQL) Get(id string) (*ActionDefinition, error) {
	var g gActionDef
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
		return nil, err
	}
	var a ActionDefinition
	if err := json.Unmarshal([]byte(g.Data), &a); err != nil {
		return nil, err
	}
	return &a, nil
}
func (s *sqlActDefStoreSQL) GetByName(name, namespace, projectID string) (*ActionDefinition, error) {
	var all []gActionDef
	if err := s.db.Find(&all).Error; err != nil {
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
	var all []gActionDef
	_ = s.db.Find(&all).Error
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
	return s.db.Save(&gActionDef{ID: a.ID, Name: a.Name, Data: blobMarshal(a)}).Error
}
func (s *sqlActDefStoreSQL) Delete(id string) error {
	return s.db.Delete(&gActionDef{}, "id = ?", id).Error
}

// --- WorkflowExecutionStore ---

type gWfEx struct {
	ID         string `gorm:"primaryKey"`
	WorkflowID string `gorm:"index"`
	State      string `gorm:"index"`
	Data       string
}

func (gWfEx) TableName() string { return "workflow_executions" }

type sqlWfExStoreSQL struct{ db *gorm.DB }

func (s *sqlWfExStoreSQL) Create(wf *WorkflowExecution) error {
	if wf.ID == "" {
		wf.ID = GenerateID()
	}
	return s.db.Create(&gWfEx{ID: wf.ID, WorkflowID: wf.WorkflowID, State: wf.State, Data: blobMarshal(wf)}).Error
}
func (s *sqlWfExStoreSQL) Get(id string) (*WorkflowExecution, error) {
	var g gWfEx
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
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
	var all []gWfEx
	_ = s.db.Find(&all).Error
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
	return s.db.Save(&gWfEx{ID: wf.ID, WorkflowID: wf.WorkflowID, State: wf.State, Data: blobMarshal(wf)}).Error
}
func (s *sqlWfExStoreSQL) UpdateState(id, curState, newState string) (*WorkflowExecution, error) {
	var g gWfEx
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
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
	if err := s.db.Save(&g).Error; err != nil {
		return nil, err
	}
	return &wf, nil
}
func (s *sqlWfExStoreSQL) Delete(id string) error { return s.db.Delete(&gWfEx{}, "id = ?", id).Error }

// --- TaskExecutionStore ---

type gTaskEx struct {
	ID                  string `gorm:"primaryKey"`
	WorkflowExecutionID string `gorm:"index"`
	UniqueKey           string `gorm:"index"`
	State               string `gorm:"index"`
	Data                string
}

func (gTaskEx) TableName() string { return "task_executions" }

type sqlTaskExStoreSQL struct{ db *gorm.DB }

func (s *sqlTaskExStoreSQL) Create(t *TaskExecution) error {
	if t.ID == "" {
		t.ID = GenerateID()
	}
	return s.db.Create(&gTaskEx{
		ID: t.ID, WorkflowExecutionID: t.WorkflowExecutionID,
		UniqueKey: t.UniqueKey, State: t.State, Data: blobMarshal(t),
	}).Error
}
func (s *sqlTaskExStoreSQL) Get(id string) (*TaskExecution, error) {
	var g gTaskEx
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
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
	var gs []gTaskEx
	q := s.db
	if workflowExecutionID != "" {
		q = q.Where("workflow_execution_id = ?", workflowExecutionID)
	}
	if state != "" {
		q = q.Where("state = ?", state)
	}
	if err := q.Find(&gs).Error; err != nil {
		return nil, err
	}
	out := make([]*TaskExecution, len(gs))
	for i := range gs {
		var t TaskExecution
		_ = json.Unmarshal([]byte(gs[i].Data), &t)
		out[i] = &t
	}
	return out, nil
}
func (s *sqlTaskExStoreSQL) GetByUniqueKey(workflowExecutionID, uniqueKey, state string) ([]*TaskExecution, error) {
	var gs []gTaskEx
	q := s.db.Where("workflow_execution_id = ? AND unique_key = ?", workflowExecutionID, uniqueKey)
	if state != "" {
		q = q.Where("state = ?", state)
	}
	if err := q.Find(&gs).Error; err != nil {
		return nil, err
	}
	out := make([]*TaskExecution, len(gs))
	for i := range gs {
		var t TaskExecution
		_ = json.Unmarshal([]byte(gs[i].Data), &t)
		out[i] = &t
	}
	return out, nil
}
func (s *sqlTaskExStoreSQL) Update(t *TaskExecution) error {
	return s.db.Save(&gTaskEx{
		ID: t.ID, WorkflowExecutionID: t.WorkflowExecutionID,
		UniqueKey: t.UniqueKey, State: t.State, Data: blobMarshal(t),
	}).Error
}
func (s *sqlTaskExStoreSQL) UpdateState(id, curState, newState string) (*TaskExecution, error) {
	var g gTaskEx
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
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
	if err := s.db.Save(&g).Error; err != nil {
		return nil, err
	}
	return &t, nil
}
func (s *sqlTaskExStoreSQL) Count(workflowExecutionID, state string) (int, error) {
	q := s.db.Model(&gTaskEx{}).Where("workflow_execution_id = ?", workflowExecutionID)
	if state != "" {
		q = q.Where("state = ?", state)
	}
	var n int64
	if err := q.Count(&n).Error; err != nil {
		return 0, err
	}
	return int(n), nil
}
func (s *sqlTaskExStoreSQL) Delete(id string) error {
	return s.db.Delete(&gTaskEx{}, "id = ?", id).Error
}

// --- ActionExecutionStore ---

type gActEx struct {
	ID              string `gorm:"primaryKey"`
	TaskExecutionID string `gorm:"index"`
	State           string
	Data            string
}

func (gActEx) TableName() string { return "action_executions" }

type sqlActExStoreSQL struct{ db *gorm.DB }

func (s *sqlActExStoreSQL) Create(a *ActionExecution) error {
	if a.ID == "" {
		a.ID = GenerateID()
	}
	return s.db.Create(&gActEx{
		ID: a.ID, TaskExecutionID: a.TaskExecutionID,
		State: a.State, Data: blobMarshal(a),
	}).Error
}
func (s *sqlActExStoreSQL) Get(id string) (*ActionExecution, error) {
	var g gActEx
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
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
	var gs []gActEx
	q := s.db
	if taskExecutionID != "" {
		q = q.Where("task_execution_id = ?", taskExecutionID)
	}
	if state != "" {
		q = q.Where("state = ?", state)
	}
	if err := q.Find(&gs).Error; err != nil {
		return nil, err
	}
	out := make([]*ActionExecution, len(gs))
	for i := range gs {
		var a ActionExecution
		_ = json.Unmarshal([]byte(gs[i].Data), &a)
		out[i] = &a
	}
	return out, nil
}
func (s *sqlActExStoreSQL) Update(a *ActionExecution) error {
	return s.db.Save(&gActEx{
		ID: a.ID, TaskExecutionID: a.TaskExecutionID,
		State: a.State, Data: blobMarshal(a),
	}).Error
}
func (s *sqlActExStoreSQL) Delete(id string) error { return s.db.Delete(&gActEx{}, "id = ?", id).Error }

// --- EnvironmentStore ---

type gEnv struct {
	ID   string `gorm:"primaryKey"`
	Name string `gorm:"index"`
	Data string
}

func (gEnv) TableName() string { return "environments" }

type sqlEnvStoreSQL struct{ db *gorm.DB }

func (s *sqlEnvStoreSQL) Create(e *Environment) error {
	if e.ID == "" {
		e.ID = GenerateID()
	}
	return s.db.Create(&gEnv{ID: e.ID, Name: e.Name, Data: blobMarshal(e)}).Error
}
func (s *sqlEnvStoreSQL) Get(id string) (*Environment, error) {
	var g gEnv
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
		return nil, err
	}
	var e Environment
	_ = json.Unmarshal([]byte(g.Data), &e)
	return &e, nil
}
func (s *sqlEnvStoreSQL) GetByName(name, projectID string) (*Environment, error) {
	var all []gEnv
	_ = s.db.Find(&all).Error
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
	var all []gEnv
	_ = s.db.Find(&all).Error
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
	return s.db.Save(&gEnv{ID: e.ID, Name: e.Name, Data: blobMarshal(e)}).Error
}
func (s *sqlEnvStoreSQL) Delete(id string) error { return s.db.Delete(&gEnv{}, "id = ?", id).Error }

// --- CronTriggerStore (hybrid: key columns + Data blob) ---

type gCronTrigger struct {
	ID                  string `gorm:"primaryKey"`
	Name                string `gorm:"index"`
	NextExecutionTime   string // ISO8601
	RemainingExecutions int
	Data                string // blob with full CronTrigger JSON
}

func (gCronTrigger) TableName() string { return "cron_triggers" }

type sqlCronTriggerStore struct{ db *gorm.DB }

func (s *sqlCronTriggerStore) Create(ct *CronTrigger) error {
	if ct.ID == "" {
		ct.ID = GenerateID()
	}
	if ct.NextExecutionTime.IsZero() && ct.IntervalSeconds > 0 {
		ct.NextExecutionTime = Now().Add(time.Duration(ct.IntervalSeconds) * time.Second)
	}
	return s.db.Create(&gCronTrigger{
		ID: ct.ID, Name: ct.Name,
		NextExecutionTime:   ct.NextExecutionTime.Format(time.RFC3339),
		RemainingExecutions: ct.RemainingExecutions,
		Data:                blobMarshal(ct),
	}).Error
}
func (s *sqlCronTriggerStore) Get(id string) (*CronTrigger, error) {
	var g gCronTrigger
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
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
	var all []gCronTrigger
	if err := s.db.Find(&all).Error; err != nil {
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
	var all []gCronTrigger
	_ = s.db.Find(&all).Error
	out := make([]*CronTrigger, 0, len(all))
	for _, g := range all {
		var ct CronTrigger
		_ = json.Unmarshal([]byte(g.Data), &ct)
		out = append(out, &ct)
	}
	return out, nil
}
func (s *sqlCronTriggerStore) Update(ct *CronTrigger) error {
	return s.db.Save(&gCronTrigger{
		ID: ct.ID, Name: ct.Name,
		NextExecutionTime:   ct.NextExecutionTime.Format(time.RFC3339),
		RemainingExecutions: ct.RemainingExecutions,
		Data:                blobMarshal(ct),
	}).Error
}
func (s *sqlCronTriggerStore) Delete(id string) error {
	return s.db.Delete(&gCronTrigger{}, "id = ?", id).Error
}

// --- ScheduledJobStore ---

type gScheduledJob struct {
	ID         string `gorm:"primaryKey"`
	JobType    string
	JobArgs    string  // JSON blob
	ExecuteAt  string  `gorm:"index:idx_execute_at_captured_at"`
	CapturedAt *string `gorm:"index:idx_execute_at_captured_at"`
	Key        string  `gorm:"index:idx_key"`
	CreatedAt  string  // ISO8601
}

func (gScheduledJob) TableName() string { return "scheduled_jobs" }

type sqlScheduledJobStore struct{ db *gorm.DB }

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
	return s.db.Create(&gScheduledJob{
		ID:         j.ID,
		JobType:    j.JobType,
		JobArgs:    string(argsBlob),
		ExecuteAt:  j.ExecuteAt.Format(time.RFC3339),
		CapturedAt: captured,
		Key:        j.Key,
		CreatedAt:  now.Format(time.RFC3339),
	}).Error
}

func scheduledJobFromGorm(g *gScheduledJob) (*ScheduledJob, error) {
	j := &ScheduledJob{
		ID:      g.ID,
		JobType: g.JobType,
		Key:     g.Key,
	}
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
	var g gScheduledJob
	if err := s.db.First(&g, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return scheduledJobFromGorm(&g)
}

func (s *sqlScheduledJobStore) List() ([]*ScheduledJob, error) {
	var gs []gScheduledJob
	if err := s.db.Find(&gs).Error; err != nil {
		return nil, err
	}
	out := make([]*ScheduledJob, 0, len(gs))
	for _, g := range gs {
		j, err := scheduledJobFromGorm(&g)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

func (s *sqlScheduledJobStore) GetDueJobs(executeCutoff time.Time, now time.Time, captureTimeout time.Duration, batchSize int) ([]*ScheduledJob, error) {
	// due = (captured_at IS NULL OR captured_at < now - captureTimeout) AND execute_at <= executeCutoff
	// executeCutoff = now - pickupJobAfter in Python (gives heapDispatcher a head-start)
	timeoutBoundary := now.Add(-captureTimeout).Format(time.RFC3339)
	cutoffStr := executeCutoff.Format(time.RFC3339)

	q := s.db.Where(
		"((captured_at IS NULL OR captured_at < ?) AND execute_at <= ?)",
		timeoutBoundary, cutoffStr,
	)
	if batchSize > 0 {
		q = q.Limit(batchSize)
	}
	var gs []gScheduledJob
	if err := q.Find(&gs).Error; err != nil {
		return nil, err
	}
	out := make([]*ScheduledJob, 0, len(gs))
	for _, g := range gs {
		j, err := scheduledJobFromGorm(&g)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

func (s *sqlScheduledJobStore) Capture(id string, oldCapturedAt *time.Time, now time.Time) (bool, error) {
	newCaptured := now.Format(time.RFC3339)

	var result *gorm.DB
	if oldCapturedAt == nil {
		// UPDATE ... SET captured_at = ? WHERE id = ? AND captured_at IS NULL
		result = s.db.Model(&gScheduledJob{}).
			Where("id = ? AND captured_at IS NULL", id).
			Update("captured_at", newCaptured)
	} else {
		// UPDATE ... SET captured_at = ? WHERE id = ? AND captured_at = ?
		oldStr := oldCapturedAt.Format(time.RFC3339)
		result = s.db.Model(&gScheduledJob{}).
			Where("id = ? AND captured_at = ?", id, oldStr).
			Update("captured_at", newCaptured)
	}
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

func (s *sqlScheduledJobStore) Delete(id string) error {
	return s.db.Delete(&gScheduledJob{}, "id = ?", id).Error
}
