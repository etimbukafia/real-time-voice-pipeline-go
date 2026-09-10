package llm

import (
	"context"
	"encoding/json"
	"time"
)

// Message is one chat message in the conversation history sent to the LLM.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	Name       string     `json:"name,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// Tool describes one function the LLM may call.
type Tool struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

// ToolFunction contains the JSON schema and metadata for one callable function.
type ToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// ToolCall is the LLM's request to invoke a function.
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function ToolCallFunc `json:"function"`
}

// ToolCallFunc holds the function name and JSON-encoded argument string.
type ToolCallFunc struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Request describes one chat-completion request sent to an LLM backend.
type Request struct {
	Model        string
	SystemPrompt string
	Messages     []Message
	MaxTokens    int
	Temperature  float64
	Tools        []Tool
}

// Chunk is one streamed unit of assistant output.
//
// We keep both the raw delta and whether this is the terminal event. That lets
// the pipeline optimize for time-to-first-token while still assembling the full
// response incrementally.
type Chunk struct {
	Text      string
	First     bool
	Final     bool
	Timestamp time.Time
	ToolCalls []ToolCall
}

type Client interface {
	Generate(ctx context.Context, req Request) (<-chan Chunk, error)
}
