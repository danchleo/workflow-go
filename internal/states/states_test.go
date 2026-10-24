package states

import "testing"

func TestIsCompleted(t *testing.T) {
	if !IsCompleted(SUCCESS) {
		t.Errorf("SUCCESS should be completed")
	}
	if !IsCompleted(ERROR) {
		t.Errorf("ERROR should be completed")
	}
	if !IsCompleted(CANCELLED) {
		t.Errorf("CANCELLED should be completed")
	}
	if !IsCompleted(SKIPPED) {
		t.Errorf("SKIPPED should be completed")
	}
	if IsCompleted(RUNNING) {
		t.Errorf("RUNNING should not be completed")
	}
	if IsCompleted(PAUSED) {
		t.Errorf("PAUSED should not be completed")
	}
}

func TestIsValidTransition(t *testing.T) {
	tests := []struct {
		from, to string
		want     bool
	}{
		{IDLE, RUNNING, true},
		{IDLE, ERROR, true},
		{IDLE, CANCELLED, true},
		{IDLE, SUCCESS, false},     // Must run first
		{WAITING, RUNNING, true},
		{WAITING, ERROR, false},    // Can't go straight from WAITING to ERROR
		{RUNNING, SUCCESS, true},
		{RUNNING, ERROR, true},
		{RUNNING, CANCELLED, true},
		{RUNNING, PAUSED, true},
		{PAUSED, RUNNING, true},
		{PAUSED, ERROR, true},
		{SUCCESS, RUNNING, false},  // Terminal
		{ERROR, RUNNING, true},    // Can retry
		{CANCELLED, RUNNING, true},
		{IDLE, IDLE, true},        // Self-transition always valid
		{UNKNOWN_STATE, RUNNING, false},
	}
	for _, tt := range tests {
		got := IsValidTransition(tt.from, tt.to)
		if got != tt.want {
			t.Errorf("IsValidTransition(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

const UNKNOWN_STATE = "MYSTERIOUS"

func TestIsPausedOrCompleted(t *testing.T) {
	if !IsPausedOrCompleted(PAUSED) {
		t.Error("PAUSED should count")
	}
	if !IsPausedOrCompleted(SUCCESS) {
		t.Error("SUCCESS should count")
	}
	if IsPausedOrCompleted(RUNNING) {
		t.Error("RUNNING should not count")
	}
}

func TestIsRunning(t *testing.T) {
	if !IsRunning(RUNNING) {
		t.Error("RUNNING should be running")
	}
	if !IsRunning(RUNNING_DELAYED) {
		t.Error("RUNNING_DELAYED should be running")
	}
	if IsRunning(IDLE) {
		t.Error("IDLE is not running")
	}
}
