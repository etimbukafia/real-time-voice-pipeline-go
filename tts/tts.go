package tts

import (
	"context"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"time"
)

// Request describes one TTS synthesis request plus the turn context it belongs to.
type Request struct {
	ModelID   string
	VoiceID   string
	Language  string
	ContextID string
}

// Chunk is one streamed output unit from the TTS engine.
type Chunk struct {
	Frame     audio.AudioFrame
	ContextID string
	Done      bool
	Timestamp time.Time
}

// Engine is the contract that any streaming TTS backend must satisfy.
type Engine interface {
	Synthesize(ctx context.Context, req Request, text <-chan string) (<-chan Chunk, error)
}
