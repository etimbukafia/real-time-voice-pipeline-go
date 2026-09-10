// stt/stt.go

package stt

import (
	"context"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"time"
)

// STT is the contract that any speech-to-text backend must satisfy.
//
// The input channel carries pointers to AudioFrame so the upstream audio
// package, VAD layer, and STT layer can all share the same frame contract.
// That keeps the stages easy to compose and avoids copying frame structs.
type STT interface {
	Transcribe(ctx context.Context, audio <-chan *audio.AudioFrame) (<-chan Transcript, error)
}

// Transcript carries one STT hypothesis or final result in the pipeline's common format.
type Transcript struct {
	Text       string
	Confidence float64
	Timestamp  time.Time
	IsFinal    bool
	StartMS    int64
	EndMS      int64
	Err        error
}
