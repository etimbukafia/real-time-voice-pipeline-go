package session

import (
	"context"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/pipeline"
	"sort"
	"sync"
)

// Manager bounds concurrency and keeps track of active sessions.
//
// The pipeline itself is single-session and latency-focused. The manager is the
// thin outer layer that lets you run many sessions safely without turning the
// core orchestration code into a global shared-state tangle.
type Manager struct {
	slots  chan struct{}
	mu     sync.Mutex
	active map[string]context.CancelFunc
}

// NewManager creates a session manager with a hard cap on concurrent live sessions.
func NewManager(maxConcurrent int) *Manager {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	return &Manager{
		slots:  make(chan struct{}, maxConcurrent),
		active: make(map[string]context.CancelFunc),
	}
}

// Run reserves a concurrency slot, tracks the session by ID, and runs the pipeline until completion.
func (m *Manager) Run(ctx context.Context, sessionID string, p *pipeline.Pipeline) (*pipeline.Summary, error) {
	if p == nil {
		return nil, fmt.Errorf("session: pipeline is required")
	}
	if sessionID == "" {
		return nil, fmt.Errorf("session: session ID is required")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case m.slots <- struct{}{}:
	}
	defer func() { <-m.slots }()

	sessionCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.active[sessionID] = cancel
	m.mu.Unlock()
	defer func() {
		cancel()
		m.mu.Lock()
		delete(m.active, sessionID)
		m.mu.Unlock()
	}()

	return p.Run(sessionCtx)
}

// Cancel stops one active session by ID and reports whether a live session was found.
func (m *Manager) Cancel(sessionID string) bool {
	m.mu.Lock()
	cancel, ok := m.active[sessionID]
	m.mu.Unlock()
	if !ok {
		return false
	}
	cancel()
	return true
}

// ActiveSessionIDs returns the currently tracked session IDs in sorted order.
func (m *Manager) ActiveSessionIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make([]string, 0, len(m.active))
	for id := range m.active {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
