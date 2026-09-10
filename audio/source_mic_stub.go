//go:build !portaudio

package audio

import (
	"context"
	"fmt"
)

func NewMicSource() (*MicSource, error) {
	return nil, fmt.Errorf("audio: microphone support requires building with -tags portaudio")
}

type MicSource struct{}

func (m *MicSource) Stream(ctx context.Context) (<-chan *AudioFrame, error) {
	return nil, fmt.Errorf("audio: microphone support requires building with -tags portaudio")
}
