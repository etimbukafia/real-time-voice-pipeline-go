package pipeline

import (
	"log"
	"time"
)

type SessionState string

const (
	StateListening    SessionState = "listening"
	StateUserSpeaking SessionState = "user_speaking"
	StateThinking     SessionState = "thinking"
	StateSpeaking     SessionState = "speaking"
	StateInterrupted  SessionState = "interrupted"
)

// Config holds the runtime knobs that shape prompting, buffering, and latency behavior.
type Config struct {
	SessionID               string
	SystemPrompt            string
	LLMModel                string
	TTSModel                string
	VoiceID                 string
	Language                string
	Temperature             float64
	MaxTokens               int
	PreRollFrames           int
	StableRepeats           int
	PhraseMinChars          int
	MaxConversationMessages int
	MaxTurnAudioBuffer      int
	Logger                  *log.Logger
}

// Summary is the session-level report produced after one pipeline run completes.
type Summary struct {
	SessionID string
	Turns     []TurnMetrics
}

// TurnMetrics records the text, timing, interruption, and error details for one user turn.
type TurnMetrics struct {
	ID                  string
	UserText            string
	AssistantText       string
	Error               string
	Interrupted         bool
	UserSpeechStartedAt time.Time
	UserSpeechEndedAt   time.Time
	TranscriptFinalAt   time.Time
	LLMStartedAt        time.Time
	LLMFirstTokenAt     time.Time
	TTSFirstAudioAt     time.Time
	PlaybackStartedAt   time.Time
	InterruptedAt       time.Time
	FinishedAt          time.Time
}

// EndOfSpeechToSTTFinal reports how long STT took to finalize after user speech ended.
func (m TurnMetrics) EndOfSpeechToSTTFinal() time.Duration {
	return durationBetween(m.UserSpeechEndedAt, m.TranscriptFinalAt)
}

// EndOfSpeechToFirstToken reports the time from speech end to the first LLM token.
func (m TurnMetrics) EndOfSpeechToFirstToken() time.Duration {
	return durationBetween(m.UserSpeechEndedAt, m.LLMFirstTokenAt)
}

// EndOfSpeechToFirstAudio reports the time from speech end to the first synthesized audio chunk.
func (m TurnMetrics) EndOfSpeechToFirstAudio() time.Duration {
	return durationBetween(m.UserSpeechEndedAt, m.TTSFirstAudioAt)
}

// EndOfSpeechToPlayback reports the time from speech end to audio actually entering playback.
func (m TurnMetrics) EndOfSpeechToPlayback() time.Duration {
	return durationBetween(m.UserSpeechEndedAt, m.PlaybackStartedAt)
}

// durationBetween returns zero when either timestamp is missing and otherwise computes the delta.
func durationBetween(start, end time.Time) time.Duration {
	if start.IsZero() || end.IsZero() {
		return 0
	}
	return end.Sub(start)
}
