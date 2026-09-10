// audio/source.go

package audio

import (
	"context"
	"log"
	"time"
)

// AudioSource is anything that produces audio frames.
// Could be a mic, a websocket, a recording file, or a simulator

type AudioSource interface {
	Stream(ctx context.Context) (<-chan *AudioFrame, error)
}

// SimulatedSource streams pre-built frames at the real mic tick rate.
// Use this for development/testing without hardware.
type SimulatedSource struct {
	Frames []AudioFrame
}

// Stream replays the configured frames at the normal frame cadence.
func (s *SimulatedSource) Stream(ctx context.Context) (<-chan *AudioFrame, error) {
	out := make(chan *AudioFrame, 10) // buffer of 10 frames (200ms)
	go func() {
		defer close(out)

		ticker := time.NewTicker(FrameDuration)
		defer ticker.Stop()

		for i := range s.Frames {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			s.Frames[i].Timestamp = time.Now()

			select {
			case <-ctx.Done():
				return
			case out <- &s.Frames[i]:
			default:
				// buffer full: drop the oldest frame
				log.Printf("audio: buffer full when trying to send frame %d", i)

				select {
				case dropped := <-out:
					log.Printf("audio: dropped oldest frame (timestamp: %v) to make room", dropped.Timestamp.Format("15:04:05.000"))
				default:
					log.Printf("audio: buffer was empty, no drop needed")
				}
				out <- &s.Frames[i]
			}
		}
	}()
	return out, nil
}
