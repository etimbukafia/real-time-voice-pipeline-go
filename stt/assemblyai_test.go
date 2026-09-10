package stt

import (
	"strings"
	"testing"
)

func TestNewAssemblyAIStreamingSTTBuildsExpectedQuery(t *testing.T) {
	client, err := NewAssemblyAIStreamingSTT(AssemblyAIStreamingConfig{
		APIKey:                       "test-key",
		RealtimeURL:                  "wss://streaming.assemblyai.com/v3/ws",
		SpeechModel:                  "universal-streaming-english",
		SampleRate:                   16000,
		FormatTurns:                  true,
		VADThreshold:                 0.5,
		EndOfTurnConfidenceThreshold: 0.7,
		MinTurnSilenceMS:             800,
		MaxTurnSilenceMS:             3600,
		KeytermsPrompt:               []string{"OpenAI", "Rowan", "OpenAI", strings.Repeat("x", 60)},
	})
	if err != nil {
		t.Fatalf("NewAssemblyAIStreamingSTT returned error: %v", err)
	}

	if got := client.query.Get("speech_model"); got != "universal-streaming-english" {
		t.Fatalf("expected speech model query, got %q", got)
	}
	if got := client.query.Get("format_turns"); got != "true" {
		t.Fatalf("expected format_turns=true, got %q", got)
	}
	keyterms := client.query["keyterms_prompt"]
	if len(keyterms) != 2 {
		t.Fatalf("expected deduped keyterms count 2, got %d", len(keyterms))
	}
	if keyterms[0] != "OpenAI" || keyterms[1] != "Rowan" {
		t.Fatalf("unexpected keyterms prompt: %#v", keyterms)
	}
}

func TestAssemblyAITranscriptStateAccumulatesAcrossProviderTurns(t *testing.T) {
	state := newAssemblyAITranscriptState()

	first, ok := state.applyTurn(assemblyAITurnEvent{
		TurnOrder:  0,
		Transcript: "tell me about",
		Words: []assemblyAITurnWord{
			{Start: 0, End: 120, Confidence: 0.9},
			{Start: 120, End: 240, Confidence: 0.8},
		},
	})
	if !ok || first.Text != "tell me about" || first.IsFinal {
		t.Fatalf("unexpected first partial: %+v ok=%t", first, ok)
	}

	second, ok := state.applyTurn(assemblyAITurnEvent{
		TurnOrder:  0,
		Transcript: "tell me about a time",
		EndOfTurn:  true,
		Words: []assemblyAITurnWord{
			{Start: 0, End: 120, Confidence: 0.9},
			{Start: 120, End: 240, Confidence: 0.8},
			{Start: 240, End: 360, Confidence: 0.7},
			{Start: 360, End: 480, Confidence: 0.6},
		},
	})
	if !ok || second.Text != "tell me about a time" || second.IsFinal {
		t.Fatalf("unexpected finalized provider turn projection: %+v ok=%t", second, ok)
	}

	third, ok := state.applyTurn(assemblyAITurnEvent{
		TurnOrder:  1,
		Transcript: "you failed",
		Words: []assemblyAITurnWord{
			{Start: 500, End: 620, Confidence: 0.95},
			{Start: 620, End: 740, Confidence: 0.85},
		},
	})
	if !ok {
		t.Fatal("expected second provider turn to produce transcript")
	}
	if third.Text != "tell me about a time you failed" {
		t.Fatalf("expected cumulative transcript, got %q", third.Text)
	}

	final := state.finalTranscript()
	if !final.IsFinal {
		t.Fatal("expected final transcript marker")
	}
	if final.Text != "tell me about a time you failed" {
		t.Fatalf("unexpected final transcript %q", final.Text)
	}
}
