package pipeline

import "time"

// appendTurn records a new turn and returns its index inside the session summary.
func (s *Summary) appendTurn(id string, startedAt time.Time) int {
	s.Turns = append(s.Turns, TurnMetrics{
		ID:                  id,
		UserSpeechStartedAt: startedAt,
	})
	return len(s.Turns) - 1
}
