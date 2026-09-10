package llm

import "testing"

func TestExtractMistralEventTextAndFinal(t *testing.T) {
	payload := []byte(`{
		"choices": [
			{
				"delta": {"content": "Hello there"},
				"finish_reason": "stop"
			}
		]
	}`)

	texts, toolCalls, final := extractMistralEvent(payload)
	if !final {
		t.Fatal("expected final event")
	}
	if len(toolCalls) != 0 {
		t.Fatalf("expected no tool calls, got %d", len(toolCalls))
	}
	if len(texts) != 1 || texts[0] != "Hello there" {
		t.Fatalf("unexpected texts: %#v", texts)
	}
}

func TestMergeToolCallDeltasBuildsArgumentsAcrossChunks(t *testing.T) {
	state := make(map[int]*ToolCall)
	first := []toolCallDelta{
		{
			Index: 0,
			ID:    "call_1",
			Type:  "function",
			Function: ToolCallFunc{
				Name:      "get_pitch_history",
				Arguments: "{\"limit\":",
			},
		},
	}
	second := []toolCallDelta{
		{
			Index: 0,
			Function: ToolCallFunc{
				Arguments: "3}",
			},
		},
	}

	if !mergeToolCallDeltas(state, first) {
		t.Fatal("expected first merge to report changes")
	}
	if !mergeToolCallDeltas(state, second) {
		t.Fatal("expected second merge to report changes")
	}

	snapshot := snapshotToolCalls(state)
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(snapshot))
	}
	if snapshot[0].Function.Name != "get_pitch_history" {
		t.Fatalf("unexpected tool name %q", snapshot[0].Function.Name)
	}
	if snapshot[0].Function.Arguments != "{\"limit\":3}" {
		t.Fatalf("unexpected arguments %q", snapshot[0].Function.Arguments)
	}
}

func TestExtractMistralEventParsesToolCalls(t *testing.T) {
	payload := []byte(`{
		"choices": [
			{
				"message": {
					"tool_calls": [
						{
							"id": "abc123",
							"type": "function",
							"function": {
								"name": "get_session_stats",
								"arguments": "{}"
							}
						}
					]
				}
			}
		]
	}`)

	_, toolCalls, final := extractMistralEvent(payload)
	if final {
		t.Fatal("did not expect final flag")
	}
	if len(toolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(toolCalls))
	}
	if !toolCalls[0].Full {
		t.Fatal("expected tool call from message payload to be marked full")
	}
	if toolCalls[0].Function.Name != "get_session_stats" {
		t.Fatalf("unexpected tool name %q", toolCalls[0].Function.Name)
	}
}
