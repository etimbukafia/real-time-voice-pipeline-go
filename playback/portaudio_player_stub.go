//go:build !portaudio

package playback

import (
	"context"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
)

func NewPortAudioPlayer() (*PortAudioPlayer, error) {
	return nil, fmt.Errorf("playback: local speaker support requires building with -tags portaudio")
}

type PortAudioPlayer struct{}

func (p *PortAudioPlayer) Play(ctx context.Context, frames <-chan audio.AudioFrame) error {
	return fmt.Errorf("playback: local speaker support requires building with -tags portaudio")
}
