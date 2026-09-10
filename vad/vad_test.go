package vad

import (
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"testing"
	"time"
)

// fakeVAD is a deterministic backend used to test the state tracker.
type fakeVAD struct {
	results []bool
	index   int
}

// Process returns the next scripted VAD decision for state-tracker tests.
func (f *fakeVAD) Process(frame *audio.AudioFrame) (bool, error) {
	if f.index >= len(f.results) {
		return false, nil
	}

	result := f.results[f.index]
	f.index++
	return result, nil
}

// Reset is a no-op because the fake VAD keeps no resettable resources.
func (f *fakeVAD) Reset() error { return nil }

// Destroy is a no-op because the fake VAD keeps no external resources.
func (f *fakeVAD) Destroy() error { return nil }

// TestNewStateTrackerClampsThresholdsToOneFrame verifies that sub-frame thresholds are rounded up.
func TestNewStateTrackerClampsThresholdsToOneFrame(t *testing.T) {
	tracker := NewStateTracker(&fakeVAD{}, 1, 1)

	if tracker.speechConfirmFrames != 1 {
		t.Fatalf("speechConfirmFrames = %d, want 1", tracker.speechConfirmFrames)
	}

	if tracker.silenceConfirmFrames != 1 {
		t.Fatalf("silenceConfirmFrames = %d, want 1", tracker.silenceConfirmFrames)
	}
}

// TestStateTrackerProducesSpeechStartAndSpeechEnd checks the normal speech start/end state machine.
func TestStateTrackerProducesSpeechStartAndSpeechEnd(t *testing.T) {
	vadBackend := &fakeVAD{
		results: []bool{true, true, false, false},
	}

	tracker := NewStateTracker(vadBackend, 40, 40) // 2 frames at 20ms/frame
	base := time.Now()

	tests := []struct {
		name      string
		timestamp time.Time
		wantEvent SpeechEvent
		wantSpeak bool
	}{
		{name: "first speech frame", timestamp: base, wantEvent: NoEvent, wantSpeak: false},
		{name: "speech confirmed", timestamp: base.Add(20 * time.Millisecond), wantEvent: SpeechStart, wantSpeak: true},
		{name: "first silence frame", timestamp: base.Add(40 * time.Millisecond), wantEvent: NoEvent, wantSpeak: true},
		{name: "silence confirmed", timestamp: base.Add(60 * time.Millisecond), wantEvent: SpeechEnd, wantSpeak: false},
	}

	for _, tc := range tests {
		event, inSpeech, err := tracker.Process(&audio.AudioFrame{Timestamp: tc.timestamp})
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if event != tc.wantEvent {
			t.Fatalf("%s: event = %v, want %v", tc.name, event, tc.wantEvent)
		}
		if inSpeech != tc.wantSpeak {
			t.Fatalf("%s: inSpeech = %v, want %v", tc.name, inSpeech, tc.wantSpeak)
		}
	}

	if !tracker.SpeechStartTime.Equal(base.Add(20 * time.Millisecond)) {
		t.Fatalf("SpeechStartTime = %v, want %v", tracker.SpeechStartTime, base.Add(20*time.Millisecond))
	}
}
