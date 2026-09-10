package pipeline

import (
	"context"
	"errors"
	"github.com/etimbukafia/real-time-voice-pipeline-go/stt"
	"testing"
	"time"
)

// TestTranscriptStabilizerCommitsStablePrefixAndFinalTail checks prefix commits plus final tail flush.
func TestTranscriptStabilizerCommitsStablePrefixAndFinalTail(t *testing.T) {
	stabilizer := NewTranscriptStabilizer(1)
	in := make(chan stt.Transcript, 4)

	go func() {
		defer close(in)
		in <- stt.Transcript{Text: "turn on", Timestamp: time.Now()}
		in <- stt.Transcript{Text: "turn on the", Timestamp: time.Now()}
		in <- stt.Transcript{Text: "turn on the fan", IsFinal: true, Timestamp: time.Now()}
	}()

	var commits []TranscriptCommit
	for commit := range stabilizer.Run(context.Background(), in) {
		commits = append(commits, commit)
	}

	if len(commits) != 2 {
		t.Fatalf("commit count = %d, want 2", len(commits))
	}
	if commits[0].Text != "turn on" || commits[0].Final {
		t.Fatalf("first commit = %#v, want non-final stable prefix", commits[0])
	}
	if commits[1].Text != "turn on the fan" || !commits[1].Final {
		t.Fatalf("second commit = %#v, want final transcript", commits[1])
	}
}

func TestTranscriptStabilizerPropagatesProviderError(t *testing.T) {
	stabilizer := NewTranscriptStabilizer(2)
	in := make(chan stt.Transcript, 1)

	go func() {
		defer close(in)
		in <- stt.Transcript{Err: errors.New("stt: upstream closed"), Timestamp: time.Now()}
	}()

	commits := make([]TranscriptCommit, 0, 1)
	for commit := range stabilizer.Run(context.Background(), in) {
		commits = append(commits, commit)
	}

	if len(commits) != 1 {
		t.Fatalf("commit count = %d, want 1", len(commits))
	}
	if commits[0].Err == nil || commits[0].Err.Error() != "stt: upstream closed" {
		t.Fatalf("commit err = %v, want propagated provider error", commits[0].Err)
	}
}
