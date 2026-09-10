package vad

import (
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"math"
)

// EnergyVAD is a lightweight fallback VAD that works without external model
// files or build tags.
//
// Why keep this in the codebase when Silero exists?
//   - It makes the project runnable out of the box, which matters for study.
//   - It gives you a "glass box" implementation you can understand fully.
//   - It is intentionally simple, so it is useful for tests and demos.
//
// This is not meant to replace Silero in production. It is the cheapest
// possible VAD that still preserves the two architectural truths we care about:
// frame-by-frame streaming and cancel-safe turn detection.
type EnergyVAD struct {
	threshold float64
}

// NewEnergyVAD creates an amplitude-threshold VAD.
//
// The threshold is normalized to the PCM16 range [0.0, 1.0].
// Typical demo values are around 0.02 to 0.08 depending on how "loud" your
// synthetic or captured audio is.
func NewEnergyVAD(threshold float64) (*EnergyVAD, error) {
	if threshold <= 0 || threshold >= 1 {
		return nil, fmt.Errorf("vad: energy threshold must be between 0 and 1, got %.3f", threshold)
	}

	return &EnergyVAD{threshold: threshold}, nil
}

// Process estimates whether a frame contains speech by measuring mean sample energy.
func (v *EnergyVAD) Process(frame *audio.AudioFrame) (bool, error) {
	if frame == nil || len(frame.Data) == 0 {
		return false, nil
	}
	if len(frame.Data)%2 != 0 {
		return false, fmt.Errorf("vad: malformed PCM16 frame: expected even number of bytes, got %d", len(frame.Data))
	}

	var sum float64
	samples := len(frame.Data) / 2

	for i := 0; i < len(frame.Data); i += 2 {
		sample := int16(frame.Data[i]) | int16(frame.Data[i+1])<<8
		sum += math.Abs(float64(sample)) / 32768.0
	}

	mean := sum / float64(samples)
	return mean >= v.threshold, nil
}

// Reset is a no-op because the energy VAD has no internal rolling state.
func (v *EnergyVAD) Reset() error { return nil }

// Destroy is a no-op because the energy VAD owns no external resources.
func (v *EnergyVAD) Destroy() error { return nil }
