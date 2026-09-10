//go:build portaudio

// audio/source_mic.go
// Real microphone source using PortAudio.
// Only compiles with: go build -tags portaudio

package audio

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gordonklaus/portaudio"
)

// MicSource owns the live PortAudio microphone stream and its reusable capture buffer.
type MicSource struct {
	stream *portaudio.Stream
	buffer []int16
}

// NewMicSource opens the default microphone and prepares a 20ms PCM capture stream.
func NewMicSource() (*MicSource, error) {
	if err := portaudio.Initialize(); err != nil {
		return nil, fmt.Errorf("audio: initialize portaudio: %w", err)
	}

	samples := make([]int16, FrameSize/2)
	stream, err := portaudio.OpenDefaultStream(1, 0, float64(SampleRate), len(samples), &samples)
	if err != nil {
		_ = portaudio.Terminate()
		return nil, fmt.Errorf("audio: open microphone stream: %w", err)
	}
	if err := stream.Start(); err != nil {
		_ = stream.Close()
		_ = portaudio.Terminate()
		return nil, fmt.Errorf("audio: start microphone stream: %w", err)
	}

	return &MicSource{
		stream: stream,
		buffer: samples,
	}, nil
}

// Stream continuously reads microphone samples and emits them as audio frames.
func (m *MicSource) Stream(ctx context.Context) (<-chan *AudioFrame, error) {
	out := make(chan *AudioFrame, 25) // buffer of 25 frames (500ms)
	go func() {
		defer close(out)

		// Three pre-allocated variables, created once before the loop, reused every iteration:

		// 1. samples: 20ms buffer that PortAudio fills with raw mic data.
		//    The stream was opened against this backing array, so Read writes
		//    directly into it without per-frame allocation.
		samples := m.buffer

		// 2. data: The byte conversion buffer. Reused every frame — no alloc per frame
		data := make([]byte, len(samples)*2)

		// 3. frameCount: counter for logging
		var frameCount uint64

		for {
			// Check if the context has been cancelled (check if we should stop)
			select {
			case <-ctx.Done():
				return // someone cancelled - exit the goroutine
			default: // not cancelled - keep going
			}

			// Read from microphone
			// blocks for 20ms while the OS fills samples from the mic
			err := m.stream.Read() // doesn't allocate — it writes (overwrites) into this existing slice every 20ms.
			if err != nil {
				log.Printf("audio: failed to read from microphone: %v", err)
				continue
			}

			// convert []int16 -> []byte (reusing pre-allocated buffer)
			// split each 2-byte int16 into its two individual bytes (little endian)
			for j, s := range samples {
				data[j*2] = byte(s)        // low byte: keeps bottom 8 bits
				data[j*2+1] = byte(s >> 8) // high byte: shifts top 8 bits down
			}

			// Copy data so the frame owns its bytes (data slice gets overwritten next Read)
			frameBuf := make([]byte, len(data))
			copy(frameBuf, data)

			frame := &AudioFrame{
				Data:      frameBuf,
				Timestamp: time.Now(),
			}
			frameCount++

			select {
			case <-ctx.Done():
				return
			case out <- frame: // success: there was room in the buffer, frame queued
			default:
				// buffer full: drop the oldest frame
				log.Printf("audio: buffer full when trying to send frame %d", frameCount)

				// safely try to drop the oldest frame by reading from out without blocking
				select {
				case dropped := <-out:
					log.Printf("audio: dropped oldest frame (timestamp: %v) to make room", dropped.Timestamp.Format("15:04:05.000"))
				default:
					log.Printf("audio: buffer was empty, no drop needed")
				}
				// Now that we know there is 100% room, send the new one
				out <- frame
			}
		}
	}()
	return out, nil
}

/* Notes:

1. Why low byte first?
   PortAudio gives us little-endian samples.
   The least significant byte (LSB) goes at the lower address.
   This matches x86/ARM CPUs natively and is the standard for PCM audio (WAV files use it too).

2. For the dropping logic:
   It's okay for a prototype, but a cleaner design later might use:
   - a ring buffer
   - a dedicated buffering component
   - or simply block instead of dropping if missing audio is unacceptable
*/
