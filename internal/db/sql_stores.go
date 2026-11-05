// Package db — gorm/gen-backed SQL CRUD for all 16 Python-parity tables.
//
// Full-schema rewrite: every Python SQLAlchemy column has a matching field
// in gen/sqlmodel/. GORM's `serializer:json` tag on map/slice columns
// (tags, input, output, ...) handles JSON marshalling transparently —
// stores never touch JSON bytes.
package db

import (
	"fmt"
	"time"

	"github.com/danchleo/workflow-go/gen/sqlmodel"
	"gorm.io/gorm"
)

// =============================================================================
// slice helpers for []string ↔ sqlmodel.JL ([]interface{})
// =============================================================================

func strsToJL(in []string) sqlmodel.JL {
	if in == nil {
		return nil
	}
	out := make(sqlmodel.JL, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}

func jlToStrs(in sqlmodel.JL) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = fmt.Sprint(v)
	}
	return out
}

func nestedSlicesToJL(in [][]interface{}) sqlmodel.JL {
	if in == nil {
		return nil
	}
	out := make(sqlmodel.JL, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

func jlToNestedSlices(in sqlmodel.JL) [][]interface{} {
	if in == nil {
		return nil
	}
	out := make([][]interface{}, 0, len(in))
	for _, v := range in {
		if arr, ok := v.([]interface{}); ok {
			out = append(out, arr)
		}
	}
	return out
}

// =============================================================================
// convert: business model ↔ sqlmodel
// =============================================================================

func wfDefToG(wf *WorkflowDefinition) *sqlmodel.GWfDefModel {
	return &sqlmodel.GWfDefModel{
		ID:             wf.ID,
		Name:           wf.Name,
		Namespace:      wf.Namespace,
		Definition:     wf.Definition,
		Spec:           wf.Spec,
		Tags:           strsToJL(wf.Tags),
		IsSystem:       wf.IsSystem,
		Checksum:       wf.Checksum,
		WorkbookName:   wf.WorkbookName,
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: wf.ProjectID, Scope: wf.Scope},
	}
}
func gToWfDef(g *sqlmodel.GWfDefModel) *WorkflowDefinition {
	return &WorkflowDefinition{
		ID: g.ID, Name: g.Name, Namespace: g.Namespace, Definition: g.Definition,
		Spec: g.Spec, Tags: jlToStrs(g.Tags), IsSystem: g.IsSystem,
		Checksum: g.Checksum, WorkbookName: g.WorkbookName,
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func actDefToG(a *ActionDefinition) *sqlmodel.GActDefModel {
	return &sqlmodel.GActDefModel{
		ID: a.ID, Name: a.Name, Namespace: a.Namespace,
		IsSystem: a.IsSystem, Description: a.Description,
		Input: a.Input, ActionClass: a.ActionClass, Attributes: a.Attributes,
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: a.ProjectID, Scope: a.Scope},
	}
}
func gToActDef(g *sqlmodel.GActDefModel) *ActionDefinition {
	return &ActionDefinition{
		ID: g.ID, Name: g.Name, Namespace: g.Namespace,
		IsSystem: g.IsSystem, Description: g.Description,
		Input: g.Input, ActionClass: g.ActionClass, Attributes: g.Attributes,
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func wfExToG(wf *WorkflowExecution) *sqlmodel.GWfExModel {
	return &sqlmodel.GWfExModel{
		ID:                wf.ID,
		Name:              wf.Name,
		Description:       wf.Description,
		WorkflowName:      wf.WorkflowName,
		WorkflowNamespace: wf.WorkflowNamespace,
		WorkflowID:        wf.WorkflowID,
		State:             wf.State,
		StateInfo:         fmt.Sprint(wf.StateInfo),
		Tags:              strsToJL(wf.Tags),
		RuntimeContext:    wf.RuntimeContext,
		Spec:              wf.Spec,
		Accepted:          wf.Accepted,
		Input:             wf.Input,
		Output:            wf.Output,
		Params:            wf.Params,
		Context:           wf.Context,
		TaskExecutionID:   wf.TaskExecutionID,
		RootExecutionID:   wf.RootExecutionID,
		SSecureColumns:    sqlmodel.SSecureColumns{ProjectID: wf.ProjectID, Scope: wf.Scope},
	}
}
func gToWfEx(g *sqlmodel.GWfExModel) *WorkflowExecution {
	return &WorkflowExecution{
		ID: g.ID, Name: g.Name, Description: g.Description,
		WorkflowName: g.WorkflowName, WorkflowNamespace: g.WorkflowNamespace,
		WorkflowID: g.WorkflowID, State: g.State, StateInfo: g.StateInfo,
		Tags: jlToStrs(g.Tags), RuntimeContext: g.RuntimeContext,
		Spec: g.Spec, Accepted: g.Accepted, Input: g.Input, Output: g.Output,
		Params: g.Params, Context: g.Context,
		TaskExecutionID: g.TaskExecutionID, RootExecutionID: g.RootExecutionID,
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func taskExToG(t *TaskExecution) *sqlmodel.GTaskExModel {
	return &sqlmodel.GTaskExModel{
		ID:                  t.ID,
		Name:                t.Name,
		Description:         t.Description,
		WorkflowName:        t.WorkflowName,
		WorkflowNamespace:   t.WorkflowNamespace,
		WorkflowID:          t.WorkflowID,
		State:               t.State,
		StateInfo:           fmt.Sprint(t.StateInfo),
		Tags:                strsToJL(t.Tags),
		RuntimeContext:      t.RuntimeContext,
		Spec:                t.Spec,
		UniqueKey:           t.UniqueKey,
		Type:                t.Type,
		StartedAt:           t.StartedAt,
		FinishedAt:          t.FinishedAt,
		Processed:           t.Processed,
		HasNextTasks:        t.HasNextTasks,
		NextTasks:           nestedSlicesToJL(t.NextTasks),
		ErrorHandled:        t.ErrorHandled,
		InContext:           t.InContext,
		Published:           t.Published,
		Output:              t.Output,
		WorkflowExecutionID: t.WorkflowExecutionID,
		SSecureColumns:      sqlmodel.SSecureColumns{ProjectID: t.ProjectID, Scope: t.Scope},
	}
}
func gToTaskEx(g *sqlmodel.GTaskExModel) *TaskExecution {
	return &TaskExecution{
		ID: g.ID, Name: g.Name, Description: g.Description,
		WorkflowName: g.WorkflowName, WorkflowNamespace: g.WorkflowNamespace,
		WorkflowID: g.WorkflowID, State: g.State, StateInfo: g.StateInfo,
		Tags: jlToStrs(g.Tags), RuntimeContext: g.RuntimeContext,
		Spec: g.Spec, UniqueKey: g.UniqueKey, Type: g.Type,
		StartedAt: g.StartedAt, FinishedAt: g.FinishedAt,
		Processed: g.Processed, HasNextTasks: g.HasNextTasks,
		NextTasks:           jlToNestedSlices(g.NextTasks),
		ErrorHandled:        g.ErrorHandled,
		InContext:           g.InContext,
		Published:           g.Published,
		Output:              g.Output,
		WorkflowExecutionID: g.WorkflowExecutionID,
		ProjectID:           g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func actExToG(a *ActionExecution) *sqlmodel.GActExModel {
	return &sqlmodel.GActExModel{
		ID:                a.ID,
		Name:              a.Name,
		Description:       a.Description,
		WorkflowName:      a.WorkflowName,
		WorkflowNamespace: a.WorkflowNamespace,
		WorkflowID:        a.WorkflowID,
		State:             a.State,
		StateInfo:         fmt.Sprint(a.StateInfo),
		RuntimeContext:    a.RuntimeContext,
		Accepted:          a.Accepted,
		Input:             a.Input,
		Output:            a.Output,
		TaskExecutionID:   a.TaskExecutionID,
		LastHeartbeat:     a.LastHeartbeat,
		IsSync:            a.IsSync,
		SSecureColumns:    sqlmodel.SSecureColumns{ProjectID: a.ProjectID, Scope: a.Scope},
	}
}
func gToActEx(g *sqlmodel.GActExModel) *ActionExecution {
	return &ActionExecution{
		ID: g.ID, Name: g.Name, Description: g.Description,
		WorkflowName: g.WorkflowName, WorkflowNamespace: g.WorkflowNamespace,
		WorkflowID: g.WorkflowID, State: g.State, StateInfo: g.StateInfo,
		RuntimeContext: g.RuntimeContext, Accepted: g.Accepted,
		Input: g.Input, Output: g.Output,
		TaskExecutionID: g.TaskExecutionID,
		LastHeartbeat:   g.LastHeartbeat, IsSync: g.IsSync,
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func wbToG(wb *Workbook) *sqlmodel.GWorkbookModel {
	return &sqlmodel.GWorkbookModel{
		ID: wb.ID, Name: wb.Name, Namespace: wb.Namespace, Definition: wb.Definition,
		Tags: strsToJL(wb.Tags), IsSystem: wb.IsSystem,
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: wb.ProjectID, Scope: wb.Scope},
	}
}
func gToWb(g *sqlmodel.GWorkbookModel) *Workbook {
	return &Workbook{
		ID: g.ID, Name: g.Name, Namespace: g.Namespace, Definition: g.Definition,
		Tags: jlToStrs(g.Tags), IsSystem: g.IsSystem,
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func envToG(e *Environment) *sqlmodel.GEnvModel {
	return &sqlmodel.GEnvModel{
		ID: e.ID, Name: e.Name, Description: e.Description, Variables: e.Variables,
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: e.ProjectID, Scope: e.Scope},
	}
}
func gToEnv(g *sqlmodel.GEnvModel) *Environment {
	return &Environment{
		ID: g.ID, Name: g.Name, Description: g.Description, Variables: g.Variables,
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func cronToG(ct *CronTrigger) *sqlmodel.GCronTriggerModel {
	return &sqlmodel.GCronTriggerModel{
		ID: ct.ID, Name: ct.Name, Pattern: ct.Pattern,
		IntervalSeconds:    ct.IntervalSeconds,
		FirstExecutionTime: ct.FirstExecutionTime, NextExecutionTime: ct.NextExecutionTime,
		WorkflowName: ct.WorkflowName, RemainingExecutions: ct.RemainingExecutions,
		WorkflowID: ct.WorkflowID, WorkflowParams: ct.WorkflowParams,
		WorkflowInput:  ct.WorkflowInput,
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: ct.ProjectID, Scope: ct.Scope},
	}
}
func gToCron(g *sqlmodel.GCronTriggerModel) *CronTrigger {
	return &CronTrigger{
		ID: g.ID, Name: g.Name, Pattern: g.Pattern,
		IntervalSeconds:    g.IntervalSeconds,
		FirstExecutionTime: g.FirstExecutionTime, NextExecutionTime: g.NextExecutionTime,
		WorkflowName: g.WorkflowName, RemainingExecutions: g.RemainingExecutions,
		WorkflowID: g.WorkflowID, WorkflowParams: g.WorkflowParams,
		WorkflowInput: g.WorkflowInput,
		ProjectID:     g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func dynActDefToG(d *DynamicActionDefinition) *sqlmodel.GDynamicActionDefModel {
	return &sqlmodel.GDynamicActionDefModel{
		ID: d.ID, Name: d.Name, Namespace: d.Namespace,
		ClassName: d.ClassName, CodeSourceID: d.CodeSourceID,
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: d.ProjectID, Scope: d.Scope},
	}
}
func gToDynActDef(g *sqlmodel.GDynamicActionDefModel) *DynamicActionDefinition {
	return &DynamicActionDefinition{
		ID: g.ID, Name: g.Name, Namespace: g.Namespace,
		ClassName: g.ClassName, CodeSourceID: g.CodeSourceID,
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func evTrigToG(e *EventTrigger) *sqlmodel.GEventTriggerModel {
	return &sqlmodel.GEventTriggerModel{
		ID: e.ID, Name: e.Name, WorkflowID: e.WorkflowID,
		WorkflowParams: e.WorkflowParams, WorkflowInput: e.WorkflowInput,
		Exchange: e.Exchange, Topic: e.Topic, Event: e.Event,
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: e.ProjectID, Scope: e.Scope},
	}
}
func gToEvTrig(g *sqlmodel.GEventTriggerModel) *EventTrigger {
	return &EventTrigger{
		ID: g.ID, Name: g.Name, WorkflowID: g.WorkflowID,
		WorkflowParams: g.WorkflowParams, WorkflowInput: g.WorkflowInput,
		Exchange: g.Exchange, Topic: g.Topic, Event: g.Event,
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

func resMemToG(m *ResourceMember) *sqlmodel.GResourceMemberModel {
	return &sqlmodel.GResourceMemberModel{
		ID: m.ID, ResourceID: m.ResourceID, ResourceType: m.ResourceType,
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: m.ProjectID},
		MemberID:       m.MemberID, Status: m.Status,
	}
}
func gToResMem(g *sqlmodel.GResourceMemberModel) *ResourceMember {
	return &ResourceMember{
		ID: g.ID, ResourceID: g.ResourceID, ResourceType: g.ResourceType,
		ProjectID: g.ProjectID, MemberID: g.MemberID, Status: g.Status,
		CreatedAt: g.CreatedAt,
	}
}

func codeSrcToG(c *CodeSource) *sqlmodel.GCodeSourceModel {
	return &sqlmodel.GCodeSourceModel{
		ID: c.ID, Name: c.Name, Content: c.Content, Version: c.Version,
		Namespace: c.Namespace, Tags: strsToJL(c.Tags),
		SSecureColumns: sqlmodel.SSecureColumns{ProjectID: c.ProjectID, Scope: c.Scope},
	}
}
func gToCodeSrc(g *sqlmodel.GCodeSourceModel) *CodeSource {
	return &CodeSource{
		ID: g.ID, Name: g.Name, Content: g.Content, Version: g.Version,
		Namespace: g.Namespace, Tags: jlToStrs(g.Tags),
		ProjectID: g.ProjectID, Scope: g.Scope, CreatedAt: g.CreatedAt,
	}
}

// =============================================================================
// Store implementations
// =============================================================================

type sqlWfDefStoreSQL struct{ store *sqlStore }

func (s *sqlWfDefStoreSQL) Create(wf *WorkflowDefinition) error {
	if wf.ID == "" {
		wf.ID = GenerateID()
	}
	return s.store.Q().GWfDefModel.WithContext(nil).Create(wfDefToG(wf))
}
func (s *sqlWfDefStoreSQL) Get(id string) (*WorkflowDefinition, error) {
	q := s.store.Q()
	g, err := q.GWfDefModel.WithContext(nil).Where(q.GWfDefModel.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "WorkflowDefinition", ID: id}
		}
		return nil, err
	}
	return gToWfDef(g), nil
}
func (s *sqlWfDefStoreSQL) GetByName(name, namespace, projectID string) (*WorkflowDefinition, error) {
	q := s.store.Q()
	qb := q.GWfDefModel.WithContext(nil).Where(q.GWfDefModel.Name.Eq(name))
	if namespace != "" {
		qb = qb.Where(q.GWfDefModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GWfDefModel.ProjectID.Eq(projectID))
	}
	g, err := qb.First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "WorkflowDefinition", ID: name}
		}
		return nil, err
	}
	return gToWfDef(g), nil
}
func (s *sqlWfDefStoreSQL) List(namespace, projectID string) ([]*WorkflowDefinition, error) {
	q := s.store.Q()
	qb := q.GWfDefModel.WithContext(nil)
	if namespace != "" {
		qb = qb.Where(q.GWfDefModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GWfDefModel.ProjectID.Eq(projectID))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*WorkflowDefinition, len(gs))
	for i, g := range gs {
		out[i] = gToWfDef(g)
	}
	return out, nil
}
func (s *sqlWfDefStoreSQL) Update(wf *WorkflowDefinition) error {
	return s.store.Q().GWfDefModel.WithContext(nil).Save(wfDefToG(wf))
}
func (s *sqlWfDefStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GWfDefModel.WithContext(nil).Where(q.GWfDefModel.ID.Eq(id)).Delete()
	return err
}

// --- ActionDefinitionStore ---

type sqlActDefStoreSQL struct{ store *sqlStore }

func (s *sqlActDefStoreSQL) Create(a *ActionDefinition) error {
	if a.ID == "" {
		a.ID = GenerateID()
	}
	return s.store.Q().GActDefModel.WithContext(nil).Create(actDefToG(a))
}
func (s *sqlActDefStoreSQL) Get(id string) (*ActionDefinition, error) {
	q := s.store.Q()
	g, err := q.GActDefModel.WithContext(nil).Where(q.GActDefModel.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "ActionDefinition", ID: id}
		}
		return nil, err
	}
	return gToActDef(g), nil
}
func (s *sqlActDefStoreSQL) GetByName(name, namespace, projectID string) (*ActionDefinition, error) {
	q := s.store.Q()
	qb := q.GActDefModel.WithContext(nil).Where(q.GActDefModel.Name.Eq(name))
	if namespace != "" {
		qb = qb.Where(q.GActDefModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GActDefModel.ProjectID.Eq(projectID))
	}
	g, err := qb.First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "ActionDefinition", ID: name}
		}
		return nil, err
	}
	return gToActDef(g), nil
}
func (s *sqlActDefStoreSQL) List(namespace, projectID string) ([]*ActionDefinition, error) {
	q := s.store.Q()
	qb := q.GActDefModel.WithContext(nil)
	if namespace != "" {
		qb = qb.Where(q.GActDefModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GActDefModel.ProjectID.Eq(projectID))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*ActionDefinition, len(gs))
	for i, g := range gs {
		out[i] = gToActDef(g)
	}
	return out, nil
}
func (s *sqlActDefStoreSQL) Update(a *ActionDefinition) error {
	return s.store.Q().GActDefModel.WithContext(nil).Save(actDefToG(a))
}
func (s *sqlActDefStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GActDefModel.WithContext(nil).Where(q.GActDefModel.ID.Eq(id)).Delete()
	return err
}

// --- WorkflowExecutionStore ---

type sqlWfExStoreSQL struct{ store *sqlStore }

func (s *sqlWfExStoreSQL) Create(wf *WorkflowExecution) error {
	if wf.ID == "" {
		wf.ID = GenerateID()
	}
	wf.Validate()
	return s.store.Q().GWfExModel.WithContext(nil).Create(wfExToG(wf))
}
func (s *sqlWfExStoreSQL) Get(id string) (*WorkflowExecution, error) {
	q := s.store.Q()
	g, err := q.GWfExModel.WithContext(nil).Where(q.GWfExModel.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "WorkflowExecution", ID: id}
		}
		return nil, err
	}
	return gToWfEx(g), nil
}
func (s *sqlWfExStoreSQL) List(projectID string) ([]*WorkflowExecution, error) {
	q := s.store.Q()
	qb := q.GWfExModel.WithContext(nil)
	if projectID != "" {
		qb = qb.Where(q.GWfExModel.ProjectID.Eq(projectID))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*WorkflowExecution, len(gs))
	for i, g := range gs {
		out[i] = gToWfEx(g)
	}
	return out, nil
}
func (s *sqlWfExStoreSQL) Update(wf *WorkflowExecution) error {
	wf.Validate()
	return s.store.Q().GWfExModel.WithContext(nil).Save(wfExToG(wf))
}
func (s *sqlWfExStoreSQL) UpdateState(id, curState, newState string) (*WorkflowExecution, error) {
	q := s.store.Q()
	g, err := q.GWfExModel.WithContext(nil).Where(q.GWfExModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	if g.State != curState {
		return nil, fmt.Errorf("state mismatch: expected %s, got %s", curState, g.State)
	}
	g.State = newState
	if err := q.GWfExModel.WithContext(nil).Save(g); err != nil {
		return nil, err
	}
	return gToWfEx(g), nil
}
func (s *sqlWfExStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GWfExModel.WithContext(nil).Where(q.GWfExModel.ID.Eq(id)).Delete()
	return err
}

// --- TaskExecutionStore ---

type sqlTaskExStoreSQL struct{ store *sqlStore }

func (s *sqlTaskExStoreSQL) Create(t *TaskExecution) error {
	if t.ID == "" {
		t.ID = GenerateID()
	}
	t.Validate()
	return s.store.Q().GTaskExModel.WithContext(nil).Create(taskExToG(t))
}
func (s *sqlTaskExStoreSQL) Get(id string) (*TaskExecution, error) {
	q := s.store.Q()
	g, err := q.GTaskExModel.WithContext(nil).Where(q.GTaskExModel.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "TaskExecution", ID: id}
		}
		return nil, err
	}
	return gToTaskEx(g), nil
}
func (s *sqlTaskExStoreSQL) List(workflowExecutionID, state string) ([]*TaskExecution, error) {
	q := s.store.Q()
	qb := q.GTaskExModel.WithContext(nil)
	if workflowExecutionID != "" {
		qb = qb.Where(q.GTaskExModel.WorkflowExecutionID.Eq(workflowExecutionID))
	}
	if state != "" {
		qb = qb.Where(q.GTaskExModel.State.Eq(state))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*TaskExecution, len(gs))
	for i, g := range gs {
		out[i] = gToTaskEx(g)
	}
	return out, nil
}
func (s *sqlTaskExStoreSQL) GetByUniqueKey(workflowExecutionID, uniqueKey, state string) ([]*TaskExecution, error) {
	q := s.store.Q()
	qb := q.GTaskExModel.WithContext(nil).Where(
		q.GTaskExModel.WorkflowExecutionID.Eq(workflowExecutionID),
		q.GTaskExModel.UniqueKey.Eq(uniqueKey),
	)
	if state != "" {
		qb = qb.Where(q.GTaskExModel.State.Eq(state))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*TaskExecution, len(gs))
	for i, g := range gs {
		out[i] = gToTaskEx(g)
	}
	return out, nil
}
func (s *sqlTaskExStoreSQL) Update(t *TaskExecution) error {
	t.Validate()
	return s.store.Q().GTaskExModel.WithContext(nil).Save(taskExToG(t))
}
func (s *sqlTaskExStoreSQL) UpdateState(id, curState, newState string) (*TaskExecution, error) {
	q := s.store.Q()
	g, err := q.GTaskExModel.WithContext(nil).Where(q.GTaskExModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	if g.State != curState {
		return nil, fmt.Errorf("state mismatch: expected %s, got %s", curState, g.State)
	}
	g.State = newState
	if err := q.GTaskExModel.WithContext(nil).Save(g); err != nil {
		return nil, err
	}
	return gToTaskEx(g), nil
}
func (s *sqlTaskExStoreSQL) Count(workflowExecutionID, state string) (int, error) {
	q := s.store.Q()
	qb := q.GTaskExModel.WithContext(nil).Where(q.GTaskExModel.WorkflowExecutionID.Eq(workflowExecutionID))
	if state != "" {
		qb = qb.Where(q.GTaskExModel.State.Eq(state))
	}
	n, err := qb.Count()
	return int(n), err
}
func (s *sqlTaskExStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GTaskExModel.WithContext(nil).Where(q.GTaskExModel.ID.Eq(id)).Delete()
	return err
}

// --- ActionExecutionStore ---

type sqlActExStoreSQL struct{ store *sqlStore }

func (s *sqlActExStoreSQL) Create(a *ActionExecution) error {
	if a.ID == "" {
		a.ID = GenerateID()
	}
	a.Validate()
	return s.store.Q().GActExModel.WithContext(nil).Create(actExToG(a))
}
func (s *sqlActExStoreSQL) Get(id string) (*ActionExecution, error) {
	q := s.store.Q()
	g, err := q.GActExModel.WithContext(nil).Where(q.GActExModel.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "ActionExecution", ID: id}
		}
		return nil, err
	}
	return gToActEx(g), nil
}
func (s *sqlActExStoreSQL) List(taskExecutionID, state string) ([]*ActionExecution, error) {
	q := s.store.Q()
	qb := q.GActExModel.WithContext(nil)
	if taskExecutionID != "" {
		qb = qb.Where(q.GActExModel.TaskExecutionID.Eq(taskExecutionID))
	}
	if state != "" {
		qb = qb.Where(q.GActExModel.State.Eq(state))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*ActionExecution, len(gs))
	for i, g := range gs {
		out[i] = gToActEx(g)
	}
	return out, nil
}
func (s *sqlActExStoreSQL) Update(a *ActionExecution) error {
	a.Validate()
	return s.store.Q().GActExModel.WithContext(nil).Save(actExToG(a))
}
func (s *sqlActExStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GActExModel.WithContext(nil).Where(q.GActExModel.ID.Eq(id)).Delete()
	return err
}

// --- WorkbookStore ---

type sqlWorkbookStore struct{ store *sqlStore }

func (s *sqlWorkbookStore) Create(wb *Workbook) error {
	if wb.ID == "" {
		wb.ID = GenerateID()
	}
	return s.store.Q().GWorkbookModel.WithContext(nil).Create(wbToG(wb))
}
func (s *sqlWorkbookStore) Get(id string) (*Workbook, error) {
	q := s.store.Q()
	g, err := q.GWorkbookModel.WithContext(nil).Where(q.GWorkbookModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	return gToWb(g), nil
}
func (s *sqlWorkbookStore) GetByName(name, namespace, projectID string) (*Workbook, error) {
	q := s.store.Q()
	qb := q.GWorkbookModel.WithContext(nil).Where(q.GWorkbookModel.Name.Eq(name))
	if namespace != "" {
		qb = qb.Where(q.GWorkbookModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GWorkbookModel.ProjectID.Eq(projectID))
	}
	g, err := qb.First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "Workbook", ID: name}
		}
		return nil, err
	}
	return gToWb(g), nil
}
func (s *sqlWorkbookStore) List(namespace, projectID string) ([]*Workbook, error) {
	q := s.store.Q()
	qb := q.GWorkbookModel.WithContext(nil)
	if namespace != "" {
		qb = qb.Where(q.GWorkbookModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GWorkbookModel.ProjectID.Eq(projectID))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*Workbook, len(gs))
	for i, g := range gs {
		out[i] = gToWb(g)
	}
	return out, nil
}
func (s *sqlWorkbookStore) Update(wb *Workbook) error {
	return s.store.Q().GWorkbookModel.WithContext(nil).Save(wbToG(wb))
}
func (s *sqlWorkbookStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GWorkbookModel.WithContext(nil).Where(q.GWorkbookModel.ID.Eq(id)).Delete()
	return err
}

// --- EnvironmentStore ---

type sqlEnvStoreSQL struct{ store *sqlStore }

func (s *sqlEnvStoreSQL) Create(e *Environment) error {
	if e.ID == "" {
		e.ID = GenerateID()
	}
	return s.store.Q().GEnvModel.WithContext(nil).Create(envToG(e))
}
func (s *sqlEnvStoreSQL) Get(id string) (*Environment, error) {
	q := s.store.Q()
	g, err := q.GEnvModel.WithContext(nil).Where(q.GEnvModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	return gToEnv(g), nil
}
func (s *sqlEnvStoreSQL) GetByName(name, projectID string) (*Environment, error) {
	q := s.store.Q()
	qb := q.GEnvModel.WithContext(nil).Where(q.GEnvModel.Name.Eq(name))
	if projectID != "" {
		qb = qb.Where(q.GEnvModel.ProjectID.Eq(projectID))
	}
	g, err := qb.First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "Environment", ID: name}
		}
		return nil, err
	}
	return gToEnv(g), nil
}
func (s *sqlEnvStoreSQL) List(projectID string) ([]*Environment, error) {
	q := s.store.Q()
	qb := q.GEnvModel.WithContext(nil)
	if projectID != "" {
		qb = qb.Where(q.GEnvModel.ProjectID.Eq(projectID))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*Environment, len(gs))
	for i, g := range gs {
		out[i] = gToEnv(g)
	}
	return out, nil
}
func (s *sqlEnvStoreSQL) Update(e *Environment) error {
	return s.store.Q().GEnvModel.WithContext(nil).Save(envToG(e))
}
func (s *sqlEnvStoreSQL) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GEnvModel.WithContext(nil).Where(q.GEnvModel.ID.Eq(id)).Delete()
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
	return s.store.Q().GCronTriggerModel.WithContext(nil).Create(cronToG(ct))
}
func (s *sqlCronTriggerStore) Get(id string) (*CronTrigger, error) {
	q := s.store.Q()
	g, err := q.GCronTriggerModel.WithContext(nil).Where(q.GCronTriggerModel.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "CronTrigger", ID: id}
		}
		return nil, err
	}
	return gToCron(g), nil
}
func (s *sqlCronTriggerStore) GetByName(name string) (*CronTrigger, error) {
	q := s.store.Q()
	g, err := q.GCronTriggerModel.WithContext(nil).Where(q.GCronTriggerModel.Name.Eq(name)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "CronTrigger", ID: name}
		}
		return nil, err
	}
	return gToCron(g), nil
}
func (s *sqlCronTriggerStore) List() ([]*CronTrigger, error) {
	gs, err := s.store.Q().GCronTriggerModel.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	out := make([]*CronTrigger, len(gs))
	for i, g := range gs {
		out[i] = gToCron(g)
	}
	return out, nil
}
func (s *sqlCronTriggerStore) Update(ct *CronTrigger) error {
	return s.store.Q().GCronTriggerModel.WithContext(nil).Save(cronToG(ct))
}
func (s *sqlCronTriggerStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GCronTriggerModel.WithContext(nil).Where(q.GCronTriggerModel.ID.Eq(id)).Delete()
	return err
}

// --- ScheduledJobStore ---

type sqlScheduledJobStore struct{ store *sqlStore }

func (s *sqlScheduledJobStore) Create(j *ScheduledJob) error {
	if j.ID == "" {
		j.ID = GenerateID()
	}
	now := j.CreatedAt
	if now.IsZero() {
		now = Now()
	}
	return s.store.Q().GScheduledJobModel.WithContext(nil).Create(&sqlmodel.GScheduledJobModel{
		ID:         j.ID,
		FuncName:   j.JobType,
		FuncArgs:   j.JobArgs,
		ExecuteAt:  j.ExecuteAt,
		CapturedAt: j.CapturedAt,
		Key:        j.Key,
		CreatedAt:  now,
	})
}
func (s *sqlScheduledJobStore) Get(id string) (*ScheduledJob, error) {
	q := s.store.Q()
	g, err := q.GScheduledJobModel.WithContext(nil).Where(q.GScheduledJobModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	return gToScheduledJob(g), nil
}
func (s *sqlScheduledJobStore) List() ([]*ScheduledJob, error) {
	gs, err := s.store.Q().GScheduledJobModel.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	out := make([]*ScheduledJob, 0, len(gs))
	for _, g := range gs {
		out = append(out, gToScheduledJob(g))
	}
	return out, nil
}
func gToScheduledJob(g *sqlmodel.GScheduledJobModel) *ScheduledJob {
	return &ScheduledJob{
		ID: g.ID, JobType: g.FuncName, JobArgs: g.FuncArgs,
		ExecuteAt: g.ExecuteAt, CapturedAt: g.CapturedAt,
		Key: g.Key, CreatedAt: g.CreatedAt,
	}
}
func (s *sqlScheduledJobStore) GetDueJobs(executeCutoff time.Time, now time.Time, captureTimeout time.Duration, batchSize int) ([]*ScheduledJob, error) {
	timeoutBoundary := now.Add(-captureTimeout)
	db := s.store.currentDB().WithContext(nil).Model(&sqlmodel.GScheduledJobModel{})
	var gs []*sqlmodel.GScheduledJobModel
	qb := db.Where("execute_at <= ?", executeCutoff).
		Where("captured_at IS NULL OR captured_at < ?", timeoutBoundary)
	if batchSize > 0 {
		qb = qb.Limit(batchSize)
	}
	if err := qb.Find(&gs).Error; err != nil {
		return nil, err
	}
	out := make([]*ScheduledJob, 0, len(gs))
	for _, g := range gs {
		out = append(out, gToScheduledJob(g))
	}
	return out, nil
}
func (s *sqlScheduledJobStore) Capture(id string, oldCapturedAt *time.Time, now time.Time) (bool, error) {
	newCaptured := now.Format(time.RFC3339)
	db := s.store.currentDB().WithContext(nil).Model(&sqlmodel.GScheduledJobModel{})
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
	_, err := q.GScheduledJobModel.WithContext(nil).Where(q.GScheduledJobModel.ID.Eq(id)).Delete()
	return err
}

// --- DynamicActionDefinitionStore ---

type sqlDynamicActionDefStore struct{ store *sqlStore }

func (s *sqlDynamicActionDefStore) Create(d *DynamicActionDefinition) error {
	if d.ID == "" {
		d.ID = GenerateID()
	}
	return s.store.Q().GDynamicActionDefModel.WithContext(nil).Create(dynActDefToG(d))
}
func (s *sqlDynamicActionDefStore) Get(id string) (*DynamicActionDefinition, error) {
	q := s.store.Q()
	g, err := q.GDynamicActionDefModel.WithContext(nil).Where(q.GDynamicActionDefModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	return gToDynActDef(g), nil
}
func (s *sqlDynamicActionDefStore) GetByName(name, namespace, projectID string) (*DynamicActionDefinition, error) {
	q := s.store.Q()
	qb := q.GDynamicActionDefModel.WithContext(nil).Where(q.GDynamicActionDefModel.Name.Eq(name))
	if namespace != "" {
		qb = qb.Where(q.GDynamicActionDefModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GDynamicActionDefModel.ProjectID.Eq(projectID))
	}
	g, err := qb.First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "DynamicActionDefinition", ID: name}
		}
		return nil, err
	}
	return gToDynActDef(g), nil
}
func (s *sqlDynamicActionDefStore) List(namespace, projectID string) ([]*DynamicActionDefinition, error) {
	q := s.store.Q()
	qb := q.GDynamicActionDefModel.WithContext(nil)
	if namespace != "" {
		qb = qb.Where(q.GDynamicActionDefModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GDynamicActionDefModel.ProjectID.Eq(projectID))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*DynamicActionDefinition, len(gs))
	for i, g := range gs {
		out[i] = gToDynActDef(g)
	}
	return out, nil
}
func (s *sqlDynamicActionDefStore) Update(d *DynamicActionDefinition) error {
	return s.store.Q().GDynamicActionDefModel.WithContext(nil).Save(dynActDefToG(d))
}
func (s *sqlDynamicActionDefStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GDynamicActionDefModel.WithContext(nil).Where(q.GDynamicActionDefModel.ID.Eq(id)).Delete()
	return err
}

// --- EventTriggerStore ---

type sqlEventTriggerStore struct{ store *sqlStore }

func (s *sqlEventTriggerStore) Create(e *EventTrigger) error {
	if e.ID == "" {
		e.ID = GenerateID()
	}
	return s.store.Q().GEventTriggerModel.WithContext(nil).Create(evTrigToG(e))
}
func (s *sqlEventTriggerStore) Get(id string) (*EventTrigger, error) {
	q := s.store.Q()
	g, err := q.GEventTriggerModel.WithContext(nil).Where(q.GEventTriggerModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	return gToEvTrig(g), nil
}
func (s *sqlEventTriggerStore) GetByExchangeTopicEvent(exchange, topic, event, projectID string) (*EventTrigger, error) {
	q := s.store.Q()
	qb := q.GEventTriggerModel.WithContext(nil).Where(
		q.GEventTriggerModel.Exchange.Eq(exchange),
		q.GEventTriggerModel.Topic.Eq(topic),
		q.GEventTriggerModel.Event.Eq(event),
	)
	if projectID != "" {
		qb = qb.Where(q.GEventTriggerModel.ProjectID.Eq(projectID))
	}
	g, err := qb.First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "EventTrigger", ID: exchange + "/" + topic + "/" + event}
		}
		return nil, err
	}
	return gToEvTrig(g), nil
}
func (s *sqlEventTriggerStore) List() ([]*EventTrigger, error) {
	gs, err := s.store.Q().GEventTriggerModel.WithContext(nil).Find()
	if err != nil {
		return nil, err
	}
	out := make([]*EventTrigger, len(gs))
	for i, g := range gs {
		out[i] = gToEvTrig(g)
	}
	return out, nil
}
func (s *sqlEventTriggerStore) Update(e *EventTrigger) error {
	return s.store.Q().GEventTriggerModel.WithContext(nil).Save(evTrigToG(e))
}
func (s *sqlEventTriggerStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GEventTriggerModel.WithContext(nil).Where(q.GEventTriggerModel.ID.Eq(id)).Delete()
	return err
}

// --- ResourceMemberStore ---

type sqlResourceMemberStore struct{ store *sqlStore }

func (s *sqlResourceMemberStore) Create(m *ResourceMember) error {
	if m.ID == "" {
		m.ID = GenerateID()
	}
	return s.store.Q().GResourceMemberModel.WithContext(nil).Create(resMemToG(m))
}
func (s *sqlResourceMemberStore) Get(id string) (*ResourceMember, error) {
	q := s.store.Q()
	g, err := q.GResourceMemberModel.WithContext(nil).Where(q.GResourceMemberModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	return gToResMem(g), nil
}
func (s *sqlResourceMemberStore) ListByResource(resourceID, resourceType string) ([]*ResourceMember, error) {
	q := s.store.Q()
	qb := q.GResourceMemberModel.WithContext(nil).Where(
		q.GResourceMemberModel.ResourceID.Eq(resourceID),
		q.GResourceMemberModel.ResourceType.Eq(resourceType),
	)
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*ResourceMember, len(gs))
	for i, g := range gs {
		out[i] = gToResMem(g)
	}
	return out, nil
}
func (s *sqlResourceMemberStore) Update(m *ResourceMember) error {
	return s.store.Q().GResourceMemberModel.WithContext(nil).Save(resMemToG(m))
}
func (s *sqlResourceMemberStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GResourceMemberModel.WithContext(nil).Where(q.GResourceMemberModel.ID.Eq(id)).Delete()
	return err
}

// --- CodeSourceStore ---

type sqlCodeSourceStore struct{ store *sqlStore }

func (s *sqlCodeSourceStore) Create(c *CodeSource) error {
	if c.ID == "" {
		c.ID = GenerateID()
	}
	return s.store.Q().GCodeSourceModel.WithContext(nil).Create(codeSrcToG(c))
}
func (s *sqlCodeSourceStore) Get(id string) (*CodeSource, error) {
	q := s.store.Q()
	g, err := q.GCodeSourceModel.WithContext(nil).Where(q.GCodeSourceModel.ID.Eq(id)).First()
	if err != nil {
		return nil, err
	}
	return gToCodeSrc(g), nil
}
func (s *sqlCodeSourceStore) GetByName(name, namespace, projectID string) (*CodeSource, error) {
	q := s.store.Q()
	qb := q.GCodeSourceModel.WithContext(nil).Where(q.GCodeSourceModel.Name.Eq(name))
	if namespace != "" {
		qb = qb.Where(q.GCodeSourceModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GCodeSourceModel.ProjectID.Eq(projectID))
	}
	g, err := qb.First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "CodeSource", ID: name}
		}
		return nil, err
	}
	return gToCodeSrc(g), nil
}
func (s *sqlCodeSourceStore) List(namespace, projectID string) ([]*CodeSource, error) {
	q := s.store.Q()
	qb := q.GCodeSourceModel.WithContext(nil)
	if namespace != "" {
		qb = qb.Where(q.GCodeSourceModel.Namespace.Eq(namespace))
	}
	if projectID != "" {
		qb = qb.Where(q.GCodeSourceModel.ProjectID.Eq(projectID))
	}
	gs, err := qb.Find()
	if err != nil {
		return nil, err
	}
	out := make([]*CodeSource, len(gs))
	for i, g := range gs {
		out[i] = gToCodeSrc(g)
	}
	return out, nil
}
func (s *sqlCodeSourceStore) Update(c *CodeSource) error {
	return s.store.Q().GCodeSourceModel.WithContext(nil).Save(codeSrcToG(c))
}
func (s *sqlCodeSourceStore) Delete(id string) error {
	q := s.store.Q()
	_, err := q.GCodeSourceModel.WithContext(nil).Where(q.GCodeSourceModel.ID.Eq(id)).Delete()
	return err
}

// --- MetricsStore ---

type sqlMetricsStore struct{ store *sqlStore }

func (s *sqlMetricsStore) GetByName(name string) (*Metrics, error) {
	q := s.store.Q()
	g, err := q.GMetricsModel.WithContext(nil).Where(q.GMetricsModel.Name.Eq(name)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, &NotFoundError{Entity: "Metrics", ID: name}
		}
		return nil, err
	}
	return &Metrics{Name: g.Name, Value: g.Value}, nil
}
func (s *sqlMetricsStore) Set(name, value string) error {
	db := s.store.currentDB().WithContext(nil).Model(&sqlmodel.GMetricsModel{})
	result := db.Where("name = ?", name).Update("value", value)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return db.Create(&sqlmodel.GMetricsModel{Name: name, Value: value}).Error
	}
	return nil
}
func (s *sqlMetricsStore) Delete(name string) error {
	q := s.store.Q()
	_, err := q.GMetricsModel.WithContext(nil).Where(q.GMetricsModel.Name.Eq(name)).Delete()
	return err
}
