// audio/frame.go - what the pipeline works with internally

package audio

import "time"

const (
	SampleRate    = 16000 // Hz
	FrameDuration = 20 * time.Millisecond
	FrameSize     = 640 // 20ms × 16kHz × 2 bytes = 640 bytes per frame
)

// AudioFrame is one timestamped chunk of PCM16 audio flowing through the pipeline.
type AudioFrame struct {
	Data      []byte    // Raw PCM16 samples (decoded from wire format)
	Timestamp time.Time // From FrameHeader.Timestamp after decoding
}
