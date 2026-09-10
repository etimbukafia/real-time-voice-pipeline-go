package pipeline

import (
	"context"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"github.com/etimbukafia/real-time-voice-pipeline-go/llm"
	"github.com/etimbukafia/real-time-voice-pipeline-go/stt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/tts"
	"github.com/etimbukafia/real-time-voice-pipeline-go/vad"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSource is a deterministic audio source used by pipeline tests.
type fakeSource struct {
	frames []*audio.AudioFrame
	delay  time.Duration
}

// Stream replays the scripted frames into the pipeline test harness.
func (s *fakeSource) Stream(ctx context.Context) (<-chan *audio.AudioFrame, error) {
	out := make(chan *audio.AudioFrame, len(s.frames))

	go func() {
		defer close(out)
		for _, frame := range s.frames {
			if s.delay > 0 {
				timer := time.NewTimer(s.delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}

			select {
			case <-ctx.Done():
				return
			case out <- frame:
			}
		}
	}()

	return out, nil
}

// scriptedVAD returns a preplanned speech/not-speech sequence for pipeline tests.
type scriptedVAD struct {
	results []bool
	index   int
}

// Process returns the next scripted speech/not-speech decision for tests.
func (v *scriptedVAD) Process(frame *audio.AudioFrame) (bool, error) {
	if v.index >= len(v.results) {
		return false, nil
	}
	result := v.results[v.index]
	v.index++
	return result, nil
}

// Reset is a no-op because the test VAD has no internal resources.
func (v *scriptedVAD) Reset() error { return nil }

// Destroy is a no-op because the test VAD has no external resources.
func (v *scriptedVAD) Destroy() error { return nil }

// spyPlayer records whether playback received frames during a test run.
type spyPlayer struct {
	frames int
}

// Play counts the frames received so tests can assert that playback actually happened.
func (p *spyPlayer) Play(ctx context.Context, frames <-chan audio.AudioFrame) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-frames:
			if !ok {
				return nil
			}
			p.frames++
		}
	}
}

// scriptedSTT emits preplanned partial and final transcripts for each test turn.
type scriptedSTT struct {
	mu      sync.Mutex
	scripts []sttScript
	index   int
}

// sttScript describes one fake STT turn, including partials, final text, and delays.
type sttScript struct {
	partials     []string
	final        string
	partialDelay time.Duration
	finalDelay   time.Duration
}

// Transcribe emits the next scripted transcript sequence after input audio closes.
func (s *scriptedSTT) Transcribe(ctx context.Context, audioStream <-chan *audio.AudioFrame) (<-chan stt.Transcript, error) {
	script := s.nextScript()
	out := make(chan stt.Transcript, len(script.partials)+1)

	go func() {
		defer close(out)

		frameCount := 0
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-audioStream:
				if !ok {
					if frameCount == 0 {
						return
					}
					for _, partial := range script.partials {
						if !sleepContext(ctx, script.partialDelay) {
							return
						}
						select {
						case <-ctx.Done():
							return
						case out <- stt.Transcript{Text: partial, Timestamp: time.Now()}:
						}
					}

					if !sleepContext(ctx, script.finalDelay) {
						return
					}
					select {
					case <-ctx.Done():
						return
					case out <- stt.Transcript{Text: script.final, IsFinal: true, Timestamp: time.Now()}:
					}
					return
				}
				frameCount++
			}
		}
	}()

	return out, nil
}

// nextScript returns the next STT script in sequence.
func (s *scriptedSTT) nextScript() sttScript {
	s.mu.Lock()
	defer s.mu.Unlock()
	script := s.scripts[s.index]
	s.index++
	return script
}

// scriptedLLM emits a preplanned assistant response as a stream of text chunks.
type scriptedLLM struct {
	mu      sync.Mutex
	scripts []llmScript
	index   int
}

// llmScript describes one fake assistant response and its chunk pacing.
type llmScript struct {
	response   string
	chunkDelay time.Duration
}

// Generate emits the next scripted assistant response as streaming chunks.
func (s *scriptedLLM) Generate(ctx context.Context, req llm.Request) (<-chan llm.Chunk, error) {
	script := s.nextScript()
	out := make(chan llm.Chunk, 8)

	go func() {
		defer close(out)
		words := strings.Fields(script.response)
		for i := 0; i < len(words); i += 3 {
			end := i + 3
			if end > len(words) {
				end = len(words)
			}
			if !sleepContext(ctx, script.chunkDelay) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case out <- llm.Chunk{
				Text:      strings.Join(words[i:end], " ") + " ",
				First:     i == 0,
				Timestamp: time.Now(),
			}:
			}
		}
		select {
		case <-ctx.Done():
			return
		case out <- llm.Chunk{Final: true, Timestamp: time.Now()}:
		}
	}()

	return out, nil
}

// nextScript returns the next LLM script in sequence.
func (s *scriptedLLM) nextScript() llmScript {
	s.mu.Lock()
	defer s.mu.Unlock()
	script := s.scripts[s.index]
	s.index++
	return script
}

// scriptedTTS turns incoming phrases into deterministic fake audio chunks for tests.
type scriptedTTS struct {
	initialDelay time.Duration
}

// Synthesize turns scripted phrase input into fake audio chunks with controllable delay.
func (s *scriptedTTS) Synthesize(ctx context.Context, req tts.Request, text <-chan string) (<-chan tts.Chunk, error) {
	out := make(chan tts.Chunk, 16)

	go func() {
		defer close(out)
		first := true
		for {
			select {
			case <-ctx.Done():
				return
			case phrase, ok := <-text:
				if !ok {
					select {
					case <-ctx.Done():
						return
					case out <- tts.Chunk{Done: true, Timestamp: time.Now()}:
					}
					return
				}

				if strings.TrimSpace(phrase) == "" {
					continue
				}

				delay := 2 * time.Millisecond
				if first {
					delay = s.initialDelay
					first = false
				}
				if !sleepContext(ctx, delay) {
					return
				}

				for i := 0; i < 2; i++ {
					select {
					case <-ctx.Done():
						return
					case out <- tts.Chunk{
						Frame: audio.AudioFrame{
							Data:      make([]byte, audio.FrameSize),
							Timestamp: time.Now(),
						},
						Timestamp: time.Now(),
					}:
					}
				}
			}
		}
	}()

	return out, nil
}

// sleepContext waits for the given delay unless cancellation happens first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// TestPipelineRunCompletesSingleTurn verifies the happy path for one full user-to-assistant turn.
func TestPipelineRunCompletesSingleTurn(t *testing.T) {
	base := time.Now().Add(-time.Second)
	frames := makeFrames(base, 8)

	player := &spyPlayer{}
	p := &Pipeline{
		Source: &fakeSource{frames: frames},
		Tracker: vad.NewStateTracker(&scriptedVAD{
			results: []bool{true, true, true, true, false, false, false, false},
		}, 40, 40),
		STT: &scriptedSTT{scripts: []sttScript{{
			partials:     []string{"turn on", "turn on the"},
			final:        "turn on the light",
			partialDelay: time.Millisecond,
			finalDelay:   time.Millisecond,
		}}},
		LLM:    &scriptedLLM{scripts: []llmScript{{response: "Sure, turning on the light now.", chunkDelay: time.Millisecond}}},
		TTS:    &scriptedTTS{initialDelay: time.Millisecond},
		Player: player,
		Config: Config{
			SessionID:      "test-single",
			StableRepeats:  1,
			PhraseMinChars: 8,
			Logger:         log.New(io.Discard, "", 0),
		},
	}

	summary, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(summary.Turns) != 1 {
		t.Fatalf("turn count = %d, want 1", len(summary.Turns))
	}

	turn := summary.Turns[0]
	if turn.UserText != "turn on the light" {
		t.Fatalf("UserText = %q, want %q", turn.UserText, "turn on the light")
	}
	if turn.AssistantText != "Sure, turning on the light now." {
		t.Fatalf("AssistantText = %q, want %q", turn.AssistantText, "Sure, turning on the light now.")
	}
	if turn.Interrupted {
		t.Fatal("expected turn not to be interrupted")
	}
	if turn.EndOfSpeechToSTTFinal() <= 0 {
		t.Fatal("expected positive STT latency")
	}
	if turn.EndOfSpeechToPlayback() <= 0 {
		t.Fatal("expected positive playback latency")
	}
	if player.frames == 0 {
		t.Fatal("expected playback to receive frames")
	}
}

// TestPipelineRunCancelsAssistantOnBargeIn verifies that a new utterance interrupts the active turn.
func TestPipelineRunCancelsAssistantOnBargeIn(t *testing.T) {
	base := time.Now().Add(-time.Second)
	frames := makeFrames(base, 18)

	player := &spyPlayer{}
	p := &Pipeline{
		Source: &fakeSource{frames: frames, delay: 5 * time.Millisecond},
		Tracker: vad.NewStateTracker(&scriptedVAD{
			results: []bool{
				true, true, true, false, false, false, false, false,
				true, true, true, false, false, false, false, false, false, false,
			},
		}, 40, 40),
		STT: &scriptedSTT{scripts: []sttScript{
			{
				partials:     []string{"what's the", "what's the weather"},
				final:        "what's the weather today",
				partialDelay: 2 * time.Millisecond,
				finalDelay:   2 * time.Millisecond,
			},
			{
				partials:     []string{"stop"},
				final:        "stop and listen",
				partialDelay: 2 * time.Millisecond,
				finalDelay:   2 * time.Millisecond,
			},
		}},
		LLM: &scriptedLLM{scripts: []llmScript{
			{response: "The weather is sunny with a light breeze today.", chunkDelay: 10 * time.Millisecond},
			{response: "I'm listening.", chunkDelay: 2 * time.Millisecond},
		}},
		TTS:    &scriptedTTS{initialDelay: 10 * time.Millisecond},
		Player: player,
		Config: Config{
			SessionID:      "test-barge-in",
			StableRepeats:  1,
			PhraseMinChars: 8,
			Logger:         log.New(io.Discard, "", 0),
		},
	}

	summary, err := p.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(summary.Turns) != 2 {
		t.Fatalf("turn count = %d, want 2", len(summary.Turns))
	}

	if !summary.Turns[0].Interrupted {
		t.Fatal("expected first turn to be interrupted")
	}
	if summary.Turns[1].Interrupted {
		t.Fatal("expected second turn not to be interrupted")
	}
	if summary.Turns[1].AssistantText != "I'm listening." {
		t.Fatalf("second assistant text = %q, want %q", summary.Turns[1].AssistantText, "I'm listening.")
	}
}

// makeFrames builds timestamped PCM test frames spaced at the normal frame duration.
func makeFrames(base time.Time, count int) []*audio.AudioFrame {
	frames := make([]*audio.AudioFrame, 0, count)
	for i := 0; i < count; i++ {
		frames = append(frames, &audio.AudioFrame{
			Data:      []byte{1, 0, 1, 0},
			Timestamp: base.Add(time.Duration(i) * audio.FrameDuration),
		})
	}
	return frames
}
