// vad/vad.go

package vad

import (
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"time"
)

// VAD processes audio frames and detects whether speech is present.
// Implementations: SileroVAD (silero.go)
type VAD interface {
	Process(frame *audio.AudioFrame) (bool, error)
	Reset() error
	Destroy() error
}

// SpeechEvent represents a transition detected by the state tracker.
type SpeechEvent int

const (
	NoEvent     SpeechEvent = iota
	SpeechStart             // silence → speech confirmed
	SpeechEnd               // speech → silence confirmed
)

func (e SpeechEvent) String() string {
	switch e {
	case SpeechStart:
		return "SpeechStart"
	case SpeechEnd:
		return "SpeechEnd"
	default:
		return "NoEvent"
	}
}

// StateTracker sits on top of a VAD and adds temporal smoothing.
// It counts consecutive speech/silence frames to avoid reacting to
// single-frame blips.
type StateTracker struct {
	vad VAD

	inSpeech      bool
	speechFrames  int // consecutive speech frames seen
	silenceFrames int // consecutive silence frames seen

	// Thresholds: how many consecutive frames needed to confirm a transition
	speechConfirmFrames  int // e.g., 4 frames = 80ms at 20ms/frame
	silenceConfirmFrames int // e.g., 20 frames = 400ms at 20ms/frame

	// Timestamps for logging/metrics
	SpeechStartTime time.Time
}

// NewStateTracker creates a tracker with your latency targets.
//   - speechStartMs: how long speech must persist before confirming (50-100ms)
//   - speechEndMs: how long silence must persist before confirming (300-500ms)
func NewStateTracker(vad VAD, speechStartMs, speechEndMs int) *StateTracker {
	frameDurationMs := int(audio.FrameDuration.Milliseconds()) // 20ms

	// We clamp to at least 1 frame so that callers cannot accidentally create
	// a "0-frame threshold" by passing a value smaller than one frame duration.
	//
	// Example:
	//   speechStartMs = 10ms
	//   frameDuration = 20ms
	//   10 / 20 = 0 using integer division
	//
	// A zero threshold would make the first matching frame trigger
	// immediately, which is usually not what you want when smoothing VAD.
	return &StateTracker{
		vad:                  vad,
		speechConfirmFrames:  atLeastOneFrame(speechStartMs / frameDurationMs), // 80/20 = 4 frames
		silenceConfirmFrames: atLeastOneFrame(speechEndMs / frameDurationMs),   // 400/20 = 20 frames
	}
}

// Process runs VAD on the frame and returns a SpeechEvent if a transition occurred.
func (st *StateTracker) Process(frame *audio.AudioFrame) (SpeechEvent, bool, error) {
	// The raw VAD only answers "speech in this frame: yes or no?"
	// The tracker is the layer that turns those noisy frame-level answers
	// into stable turn events such as SpeechStart and SpeechEnd.
	isSpeech, err := st.vad.Process(frame)
	if err != nil {
		return NoEvent, false, err
	}

	if isSpeech {
		st.speechFrames++
		st.silenceFrames = 0
	} else {
		st.silenceFrames++
		st.speechFrames = 0
	}

	// Transition: silence → speech
	if !st.inSpeech && st.speechFrames >= st.speechConfirmFrames {
		st.inSpeech = true
		st.SpeechStartTime = frame.Timestamp
		return SpeechStart, true, nil
	}

	// Transition: speech → silence
	if st.inSpeech && st.silenceFrames >= st.silenceConfirmFrames {
		st.inSpeech = false
		return SpeechEnd, false, nil
	}

	return NoEvent, st.inSpeech, nil
}

// Reset clears both the tracker state and the underlying VAD state.
func (st *StateTracker) Reset() error {
	st.inSpeech = false
	st.speechFrames = 0
	st.silenceFrames = 0
	return st.vad.Reset()
}

// Destroy releases the underlying VAD resources.
func (st *StateTracker) Destroy() error {
	return st.vad.Destroy()
}

// atLeastOneFrame prevents a smoothing threshold from collapsing to zero frames.
func atLeastOneFrame(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
