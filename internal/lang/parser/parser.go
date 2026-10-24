// Package parser provides YAML parsing and specification caching utilities.
// Ported from mistral-master/mistral/lang/parser.py
package parser

import (
	"fmt"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/mistral/workflow/internal/lang/v2"
)

const (
	// V2_0 is the only supported DSL version.
	V2_0 = "2.0"
)

// Cache for workflow specs by workflow execution ID.
var (
	wfExCache    = make(map[string]*v2.WorkflowSpec)
	wfExCacheMu  sync.RWMutex
	wfDefCache   = make(map[string]*v2.WorkflowSpec)
	wfDefCacheMu sync.RWMutex
)

// ParseYAML parses YAML text into a generic map.
func ParseYAML(text string) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := yaml.Unmarshal([]byte(text), &result); err != nil {
		return nil, fmt.Errorf("YAML parse error: %w", err)
	}
	if result == nil {
		return make(map[string]interface{}), nil
	}
	return result, nil
}

// GetWorkbookSpec parses a full workbook (actions + workflows).
func GetWorkbookSpec(text string) (*v2.WorkbookSpec, error) {
	data, err := ParseYAML(text)
	if err != nil {
		return nil, err
	}
	return v2.NewWorkbookSpec(data)
}

// GetWorkflowSpecFromYAML parses a single workflow definition.
func GetWorkflowSpecFromYAML(name, text string) (*v2.WorkflowSpec, error) {
	data, err := ParseYAML(text)
	if err != nil {
		return nil, err
	}
	return v2.NewWorkflowSpec(name, data)
}

// GetWorkflowSpecFromMap creates a workflow spec from a parsed map.
func GetWorkflowSpecFromMap(name string, data map[string]interface{}) (*v2.WorkflowSpec, error) {
	return v2.NewWorkflowSpec(name, data)
}

// GetTaskSpecFromMap creates a task spec from a parsed map.
func GetTaskSpecFromMap(name string, data map[string]interface{}, workflowType string) *v2.TaskSpec {
	return v2.NewTaskSpec(name, data, workflowType)
}

// Cache operations.

// CacheWorkflowSpecByExecution stores a workflow spec in the execution cache.
func CacheWorkflowSpecByExecution(wfExID string, spec *v2.WorkflowSpec) {
	wfExCacheMu.Lock()
	defer wfExCacheMu.Unlock()
	wfExCache[wfExID] = spec
}

// GetCachedWorkflowSpecByExecution retrieves a cached workflow spec.
func GetCachedWorkflowSpecByExecution(wfExID string) (*v2.WorkflowSpec, bool) {
	wfExCacheMu.RLock()
	defer wfExCacheMu.RUnlock()
	spec, ok := wfExCache[wfExID]
	return spec, ok
}

// CacheWorkflowSpecByDef stores a workflow spec in the definition cache.
func CacheWorkflowSpecByDef(wfDefID string, spec *v2.WorkflowSpec) {
	wfDefCacheMu.Lock()
	defer wfDefCacheMu.Unlock()
	wfDefCache[wfDefID] = spec
}

// GetCachedWorkflowSpecByDef retrieves a cached workflow spec.
func GetCachedWorkflowSpecByDef(wfDefID string) (*v2.WorkflowSpec, bool) {
	wfDefCacheMu.RLock()
	defer wfDefCacheMu.RUnlock()
	spec, ok := wfDefCache[wfDefID]
	return spec, ok
}

// ClearCaches clears all spec caches.
func ClearCaches() {
	wfExCacheMu.Lock()
	wfExCache = make(map[string]*v2.WorkflowSpec)
	wfExCacheMu.Unlock()

	wfDefCacheMu.Lock()
	wfDefCache = make(map[string]*v2.WorkflowSpec)
	wfDefCacheMu.Unlock()
}

// WfExCacheSize returns the number of cached execution specs.
func WfExCacheSize() int {
	wfExCacheMu.RLock()
	defer wfExCacheMu.RUnlock()
	return len(wfExCache)
}

// WfDefCacheSize returns the number of cached definition specs.
func WfDefCacheSize() int {
	wfDefCacheMu.RLock()
	defer wfDefCacheMu.RUnlock()
	return len(wfDefCache)
}
