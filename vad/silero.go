//go:build silero

// vad/silero.go

package vad

import (
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"

	"github.com/streamer45/silero-vad-go/speech"
)

// SileroVAD wraps the silero-vad-go speech detector to work
// frame-by-frame in our real-time pipeline.
type SileroVAD struct {
	detector *speech.Detector
	buf      []float32 // reusable conversion buffer
}

// NewSileroVAD creates a new VAD backed by the Silero ONNX model.
func NewSileroVAD(modelPath string) (*SileroVAD, error) {
	sd, err := speech.NewDetector(speech.DetectorConfig{
		ModelPath:            modelPath,
		SampleRate:           audio.SampleRate,
		Threshold:            0.5, // probability threshold for speech
		MinSilenceDurationMs: 300, // match speech-end target: 300ms
		SpeechPadMs:          30,  // pad speech segments by 30ms
	})
	if err != nil {
		return nil, fmt.Errorf("vad: failed to create silero detector: %w", err)
	}

	return &SileroVAD{
		detector: sd,
		buf:      make([]float32, audio.FrameSize/2), // pre-allocate for 320 samples
	}, nil
}

// Process takes one audio frame, converts PCM16 bytes to float32,
// and returns true if speech is detected.
func (v *SileroVAD) Process(frame *audio.AudioFrame) (bool, error) {
	if len(frame.Data) == 0 {
		// Empty audio is not speech, but it is also not a fatal error.
		// Returning nil here keeps the pipeline resilient to occasional gaps.
		return false, nil
	}

	if len(frame.Data)%2 != 0 {
		// PCM16 must arrive as pairs of bytes.
		// If we read an odd number of bytes, the frame is malformed and the
		// byte-to-sample conversion below would read past the slice boundary.
		return false, fmt.Errorf("vad: malformed PCM16 frame: expected even number of bytes, got %d", len(frame.Data))
	}

	n := len(frame.Data) / 2 // number of samples (2 bytes per int16)

	// resize buffer if needed (only on first call or if frame size changes)
	if cap(v.buf) < n {
		v.buf = make([]float32, n)
	}
	// In Process():
	v.buf = v.buf[:n]

	// convert PCM16 bytes to float32 samples
	// writes directly into v.buf's memory
	// v.buf now contains the float32 samples
	pcm16ToFloat32(v.buf, frame.Data)

	// Detect returns speech segments found in this chunk.
	// If any segments are returned, speech is happening.
	segments, err := v.detector.Detect(v.buf)
	if err != nil {
		return false, fmt.Errorf("vad: detect failed: %w", err)
	}

	return len(segments) > 0, nil
}

// Reset clears the detector's internal state (call between utterances if needed).
func (v *SileroVAD) Reset() error {
	return v.detector.Reset()
}

// Destroy releases the ONNX runtime resources. Call when shutting down.
func (v *SileroVAD) Destroy() error {
	return v.detector.Destroy()
}

// pcm16ToFloat32 converts raw PCM16 little-endian bytes into float32 samples
// in the range [-1.0, 1.0]. Writes into the pre-allocated out buffer.
func pcm16ToFloat32(out []float32, data []byte) {
	for i := 0; i < len(data); i += 2 {
		sample := int16(data[i]) | int16(data[i+1])<<8 // reassamble 2 bytes -> int16
		out[i/2] = float32(sample) / 32768.0           // normalize to [-1.0, 1.0]
	}
}
