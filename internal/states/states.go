// Package states defines valid task and workflow states and transition rules.
// Ported from mistral-master/mistral/workflow/states.py
package states

// Valid states for tasks, actions, and workflows.
const (
	IDLE           = "IDLE"           // Task is not started yet.
	WAITING        = "WAITING"        // Preconditions not met, ready when satisfied.
	RUNNING        = "RUNNING"        // Currently being executed.
	RUNNING_DELAYED = "DELAYED"       // Running but temporarily delayed.
	PAUSED         = "PAUSED"         // Paused.
	SUCCESS        = "SUCCESS"        // Finished successfully.
	CANCELLED      = "CANCELLED"      // Cancelled.
	ERROR          = "ERROR"          // Finished with an error.
	SKIPPED        = "SKIPPED"        // Skipped.
)

// All known states.
var All = []string{
	IDLE, WAITING, RUNNING, RUNNING_DELAYED, PAUSED, SUCCESS, CANCELLED, ERROR, SKIPPED,
}

// Terminal states are those that cannot transition further except where noted.
var TerminalStates = map[string]bool{
	SUCCESS:   true,
	ERROR:     true,
	CANCELLED: true,
}

// Valid state transitions: from_state -> list of allowed to_states.
var validTransitions = map[string][]string{
	IDLE:           {RUNNING, ERROR, CANCELLED},
	WAITING:        {RUNNING},
	RUNNING:        {PAUSED, RUNNING_DELAYED, SUCCESS, ERROR, CANCELLED},
	RUNNING_DELAYED: {RUNNING, ERROR, CANCELLED},
	PAUSED:         {RUNNING, ERROR, CANCELLED},
	SUCCESS:        {},
	CANCELLED:      {RUNNING},
	ERROR:          {RUNNING, SKIPPED},
}

// IsValid reports whether state is a known state.
func IsValid(state string) bool {
	for _, s := range All {
		if s == state {
			return true
		}
	}
	return false
}

// IsInvalid is the negation of IsValid.
func IsInvalid(state string) bool {
	return !IsValid(state)
}

// IsCompleted reports whether state is terminal (SUCCESS, ERROR, CANCELLED, SKIPPED).
func IsCompleted(state string) bool {
	return state == SUCCESS || state == ERROR || state == CANCELLED || state == SKIPPED
}

// IsCancelled reports whether state is CANCELLED.
func IsCancelled(state string) bool {
	return state == CANCELLED
}

// IsSkipped reports whether state is SKIPPED.
func IsSkipped(state string) bool {
	return state == SKIPPED
}

// IsRunning reports whether state is RUNNING or DELAYED.
func IsRunning(state string) bool {
	return state == RUNNING || state == RUNNING_DELAYED
}

// IsWaiting reports whether state is WAITING.
func IsWaiting(state string) bool {
	return state == WAITING
}

// IsIdle reports whether state is IDLE.
func IsIdle(state string) bool {
	return state == IDLE
}

// IsPaused reports whether state is PAUSED.
func IsPaused(state string) bool {
	return state == PAUSED
}

// IsPausedOrCompleted reports whether state is PAUSED or terminal.
func IsPausedOrCompleted(state string) bool {
	return IsPaused(state) || IsCompleted(state)
}

// IsCancelledOrSkipped reports whether state is CANCELLED or SKIPPED.
func IsCancelledOrSkipped(state string) bool {
	return IsCancelled(state) || IsSkipped(state)
}

// IsPausedOrIdle reports whether state is PAUSED or IDLE.
func IsPausedOrIdle(state string) bool {
	return IsPaused(state) || IsIdle(state)
}

// IsValidTransition reports whether a transition from fromState to toState is valid.
func IsValidTransition(fromState, toState string) bool {
	if IsInvalid(fromState) || IsInvalid(toState) {
		return false
	}

	if fromState == toState {
		return true
	}

	allowed, ok := validTransitions[fromState]
	if !ok {
		return false
	}

	for _, s := range allowed {
		if s == toState {
			return true
		}
	}
	return false
}
