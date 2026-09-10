package pipeline

import (
	"context"
	"github.com/etimbukafia/real-time-voice-pipeline-go/stt"
	"strings"
	"time"
)

// TranscriptCommit is a trusted piece of transcript text that downstream stages may safely consume.
type TranscriptCommit struct {
	Text       string
	Delta      string
	Final      bool
	Timestamp  time.Time
	Confidence float64
	Err        error
}

// TranscriptStabilizer turns noisy cumulative partial transcripts into commits.
//
// STT partials are valuable for latency, but they are not stable enough to feed
// downstream blindly. A realtime recognizer often emits:
//
//	"turn"
//	"turn on"
//	"turn on the"
//	"turn on the fan"
//
// If the LLM reacted to every update, we would waste latency and money
// re-planning the same turn over and over. The stabilizer's job is to withhold
// text until a prefix survives long enough to be trusted.
type TranscriptStabilizer struct {
	stableRepeats int
}

// NewTranscriptStabilizer creates a stabilizer that waits for repeated prefixes before committing them.
func NewTranscriptStabilizer(stableRepeats int) *TranscriptStabilizer {
	if stableRepeats < 1 {
		stableRepeats = 1
	}
	return &TranscriptStabilizer{stableRepeats: stableRepeats}
}

// Run consumes cumulative STT updates and emits only trusted transcript commits.
func (s *TranscriptStabilizer) Run(ctx context.Context, in <-chan stt.Transcript) <-chan TranscriptCommit {
	out := make(chan TranscriptCommit, 8)

	go func() {
		defer close(out)

		var committed string
		var lastPartial string
		var lastCandidate string
		repeatCount := 0

		for {
			select {
			case <-ctx.Done():
				return
			case transcript, ok := <-in:
				if !ok {
					return
				}
				if transcript.Err != nil {
					_ = sendCommit(ctx, out, TranscriptCommit{
						Err:       transcript.Err,
						Timestamp: nonZeroTime(transcript.Timestamp),
					})
					return
				}

				text := normalizeTranscript(transcript.Text)
				if text == "" {
					continue
				}

				if transcript.IsFinal {
					if strings.HasPrefix(text, committed) {
						delta := strings.TrimSpace(text[len(committed):])
						if delta != "" {
							if !sendCommit(ctx, out, TranscriptCommit{
								Text:       text,
								Delta:      delta,
								Final:      true,
								Timestamp:  nonZeroTime(transcript.Timestamp),
								Confidence: transcript.Confidence,
							}) {
								return
							}
						} else {
							if !sendCommit(ctx, out, TranscriptCommit{
								Text:       text,
								Final:      true,
								Timestamp:  nonZeroTime(transcript.Timestamp),
								Confidence: transcript.Confidence,
							}) {
								return
							}
						}
						return
					}

					// If the final transcript does not extend the committed prefix,
					// we trust the final transcript and hand it downstream whole.
					// This is the "correctness beats premature cleverness" branch:
					// one rare correction is better than preserving a stale partial.
					if !sendCommit(ctx, out, TranscriptCommit{
						Text:       text,
						Delta:      text,
						Final:      true,
						Timestamp:  nonZeroTime(transcript.Timestamp),
						Confidence: transcript.Confidence,
					}) {
						return
					}
					return
				}

				candidate := stableBoundaryPrefix(lastPartial, text)
				lastPartial = text
				if len(candidate) <= len(committed) {
					continue
				}

				if candidate == lastCandidate {
					repeatCount++
				} else {
					lastCandidate = candidate
					repeatCount = 1
				}

				if repeatCount < s.stableRepeats {
					continue
				}

				delta := strings.TrimSpace(candidate[len(committed):])
				if delta == "" {
					continue
				}

				committed = candidate
				if !sendCommit(ctx, out, TranscriptCommit{
					Text:       committed,
					Delta:      delta,
					Final:      false,
					Timestamp:  nonZeroTime(transcript.Timestamp),
					Confidence: transcript.Confidence,
				}) {
					return
				}
			}
		}
	}()

	return out
}

// stableBoundaryPrefix returns the longest shared prefix that ends at a safe split boundary.
func stableBoundaryPrefix(previous, current string) string {
	if previous == "" || current == "" {
		return ""
	}

	limit := len(previous)
	if len(current) < limit {
		limit = len(current)
	}

	i := 0
	for i < limit && previous[i] == current[i] {
		i++
	}

	prefix := current[:i]
	if i == len(previous) || i == len(current) {
		return strings.TrimSpace(prefix)
	}
	cut := strings.LastIndexFunc(prefix, isSafeBoundary)
	if cut == -1 {
		return ""
	}
	return strings.TrimSpace(prefix[:cut+1])
}

// isSafeBoundary reports whether a rune is a sensible place to commit transcript text.
func isSafeBoundary(r rune) bool {
	switch r {
	case ' ', '\n', '\t', '.', ',', '?', '!', ';', ':':
		return true
	default:
		return false
	}
}

// normalizeTranscript collapses repeated whitespace so partial comparisons stay stable.
func normalizeTranscript(text string) string {
	fields := strings.Fields(text)
	return strings.Join(fields, " ")
}

// sendCommit forwards one transcript commit unless cancellation has already won the race.
func sendCommit(ctx context.Context, out chan<- TranscriptCommit, commit TranscriptCommit) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- commit:
		return true
	}
}
