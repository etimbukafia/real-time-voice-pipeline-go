package pipeline

import "github.com/etimbukafia/real-time-voice-pipeline-go/audio"

// preRollBuffer keeps a small window of the most recent frames before VAD has
// fully confirmed speech.
//
// This looks minor, but it fixes a very common realtime bug: clipped first
// phonemes. We intentionally wait 50-100ms before trusting a speech start event
// because raw VAD is noisy. Without pre-roll, that safety delay would cut off
// the user's opening consonants before STT ever sees them.
type preRollBuffer struct {
	max    int
	frames []*audio.AudioFrame
}

// newPreRollBuffer allocates the fixed-size buffer that keeps the most recent pre-speech frames.
func newPreRollBuffer(max int) *preRollBuffer {
	if max < 0 {
		max = 0
	}
	return &preRollBuffer{max: max}
}

// Add records the newest frame and evicts the oldest one when the buffer is full.
func (b *preRollBuffer) Add(frame *audio.AudioFrame) {
	if b.max == 0 || frame == nil {
		return
	}
	if len(b.frames) == b.max {
		copy(b.frames, b.frames[1:])
		b.frames[len(b.frames)-1] = frame
		return
	}
	b.frames = append(b.frames, frame)
}

// Snapshot returns a copy of the buffered frames in capture order for replay into a new turn.
func (b *preRollBuffer) Snapshot() []*audio.AudioFrame {
	if len(b.frames) == 0 {
		return nil
	}
	out := make([]*audio.AudioFrame, len(b.frames))
	copy(out, b.frames)
	return out
}
