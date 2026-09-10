//go:build !silero

package vad

import (
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
)

// SileroVAD is the same public type as the real implementation, but in
// default builds it acts as a stub.
//
// Why keep a stub?
//   - `go test ./...` should work even on machines that do not have the
//     native/runtime pieces required by the Silero dependency.
//   - callers still get a clear error message explaining how to enable the
//     real implementation.
type SileroVAD struct{}

// NewSileroVAD reports that the real Silero implementation is disabled in this build.
func NewSileroVAD(modelPath string) (*SileroVAD, error) {
	return nil, fmt.Errorf(
		"vad: Silero VAD is disabled in the default build; rebuild with `-tags silero` to enable the real implementation",
	)
}

// Process reports that the real Silero implementation is unavailable in this build.
func (v *SileroVAD) Process(frame *audio.AudioFrame) (bool, error) {
	return false, fmt.Errorf("vad: Silero VAD is unavailable in this build")
}

// Reset is a no-op in the stub implementation.
func (v *SileroVAD) Reset() error {
	return nil
}

// Destroy is a no-op in the stub implementation.
func (v *SileroVAD) Destroy() error {
	return nil
}
