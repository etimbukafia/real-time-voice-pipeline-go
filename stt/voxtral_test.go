package stt

import (
	"testing"
	"time"
)

// TestDecodeMistralRealtimeEventBuildsPartialTranscript verifies delta decoding into cumulative text.
func TestDecodeMistralRealtimeEventBuildsPartialTranscript(t *testing.T) {
	payload := []byte(`{"type":"transcription_stream.text_delta","text":"hello ","confidence":0.91,"start_ms":0,"end_ms":120}`)

	transcript, action, ok := decodeMistralRealtimeEvent(payload, "")
	if !ok {
		t.Fatal("decodeMistralRealtimeEvent returned ok=false")
	}

	if action != mistralRealtimeActionDelta {
		t.Fatalf("action = %v, want %v", action, mistralRealtimeActionDelta)
	}

	if transcript.Text != "hello " {
		t.Fatalf("Text = %q, want %q", transcript.Text, "hello ")
	}

	if transcript.IsFinal {
		t.Fatal("IsFinal = true, want false")
	}
}

// TestDecodeMistralRealtimeEventBuildsFinalTranscriptFromAccumulatedText verifies final event decoding.
func TestDecodeMistralRealtimeEventBuildsFinalTranscriptFromAccumulatedText(t *testing.T) {
	payload := []byte(`{"type":"transcription_stream.done","confidence":0.97,"timestamp_ms":1234}`)

	transcript, action, ok := decodeMistralRealtimeEvent(payload, "hello world")
	if !ok {
		t.Fatal("decodeMistralRealtimeEvent returned ok=false")
	}

	if action != mistralRealtimeActionDone {
		t.Fatalf("action = %v, want %v", action, mistralRealtimeActionDone)
	}

	if transcript.Text != "hello world" {
		t.Fatalf("Text = %q, want %q", transcript.Text, "hello world")
	}

	if !transcript.IsFinal {
		t.Fatal("IsFinal = false, want true")
	}

	if transcript.Timestamp.UnixMilli() != 1234 {
		t.Fatalf("Timestamp = %v, want unix ms 1234", transcript.Timestamp)
	}
}

// TestMistralEventTimestampFallsBackToNow verifies the local-time fallback for bad provider timestamps.
func TestMistralEventTimestampFallsBackToNow(t *testing.T) {
	before := time.Now().Add(-time.Second)
	ts := mistralEventTimestamp(0, "not-a-time")
	after := time.Now().Add(time.Second)

	if ts.Before(before) || ts.After(after) {
		t.Fatalf("timestamp fallback produced unexpected time %v", ts)
	}
}
