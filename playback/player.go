package playback

import (
	"context"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"log"
	"time"
)

// Player is the final sink in the pipeline.
//
// The pipeline deliberately does not know whether "playback" means a speaker,
// a websocket back to Twilio, a file sink, or a black-hole test sink. That
// boundary keeps the orchestration logic clean.
type Player interface {
	Play(ctx context.Context, frames <-chan audio.AudioFrame) error
}

// LoggingPlayer simulates playback and emits a compact log line for each frame.
//
// It defaults to real-time pacing so the demo behaves like an actual live
// system. Tests can disable pacing to finish quickly.
type LoggingPlayer struct {
	Logger   *log.Logger
	Realtime bool
}

// Play drains audio frames and logs each playback event instead of sending them to a real sink.
func (p *LoggingPlayer) Play(ctx context.Context, frames <-chan audio.AudioFrame) error {
	logger := p.Logger
	if logger == nil {
		logger = log.Default()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame, ok := <-frames:
			if !ok {
				return nil
			}

			if p.Realtime {
				timer := time.NewTimer(audio.FrameDuration)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}

			logger.Printf("playback: wrote %d bytes at %s", len(frame.Data), frame.Timestamp.Format(time.RFC3339Nano))
		}
	}
}
