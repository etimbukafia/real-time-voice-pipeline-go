//go:build portaudio

package playback

import (
	"context"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"

	"github.com/gordonklaus/portaudio"
)

// PortAudioPlayer owns the live speaker stream and a reusable buffer for PCM conversion.
type PortAudioPlayer struct {
	stream  *portaudio.Stream
	buffer  []int16
	pending []byte
}

// NewPortAudioPlayer opens the default speaker output and prepares a reusable PCM buffer.
func NewPortAudioPlayer() (*PortAudioPlayer, error) {
	if err := portaudio.Initialize(); err != nil {
		return nil, fmt.Errorf("playback: initialize portaudio: %w", err)
	}

	samples := make([]int16, audio.FrameSize/2)
	stream, err := portaudio.OpenDefaultStream(0, 1, float64(audio.SampleRate), len(samples), &samples)
	if err != nil {
		_ = portaudio.Terminate()
		return nil, fmt.Errorf("playback: open output stream: %w", err)
	}
	if err := stream.Start(); err != nil {
		_ = stream.Close()
		_ = portaudio.Terminate()
		return nil, fmt.Errorf("playback: start output stream: %w", err)
	}

	return &PortAudioPlayer{
		stream: stream,
		buffer: samples,
	}, nil
}

// Play writes streamed PCM frames to the local audio device until the stream or context ends.
func (p *PortAudioPlayer) Play(ctx context.Context, frames <-chan audio.AudioFrame) error {
	blockBytes := len(p.buffer) * 2
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame, ok := <-frames:
			if !ok {
				if len(p.pending) > 0 {
					if err := p.writeBlock(p.pending, true); err != nil {
						return err
					}
					p.pending = p.pending[:0]
				}
				return nil
			}

			if len(frame.Data)%2 != 0 {
				return fmt.Errorf("playback: malformed PCM16 frame with odd byte length %d", len(frame.Data))
			}
			if len(frame.Data) == 0 {
				continue
			}

			p.pending = append(p.pending, frame.Data...)
			for len(p.pending) >= blockBytes {
				if err := p.writeBlock(p.pending[:blockBytes], false); err != nil {
					return err
				}
				p.pending = append(p.pending[:0], p.pending[blockBytes:]...)
			}
		}
	}
}

func (p *PortAudioPlayer) writeBlock(block []byte, pad bool) error {
	for i := range p.buffer {
		p.buffer[i] = 0
	}

	limit := len(block) / 2
	if limit > len(p.buffer) {
		limit = len(p.buffer)
	}
	for i := 0; i < limit; i++ {
		lo := block[i*2]
		hi := block[i*2+1]
		p.buffer[i] = int16(lo) | int16(hi)<<8
	}
	if !pad && len(block) != len(p.buffer)*2 {
		return fmt.Errorf("playback: expected full block of %d bytes, got %d", len(p.buffer)*2, len(block))
	}
	if err := p.stream.Write(); err != nil {
		return fmt.Errorf("playback: write audio: %w", err)
	}
	return nil
}
