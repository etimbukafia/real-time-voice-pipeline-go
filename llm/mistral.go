package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// MistralClient streams chat completions from Mistral's `/v1/chat/completions`
// endpoint.
//
// The official API documents streaming responses as data-only server-sent
// events. The exact JSON event schema can evolve, so the parser below is
// intentionally tolerant: it extracts text from the common delta/message
// shapes rather than binding the whole stream to one brittle struct.
type MistralClient struct {
	APIKey     string
	BaseURL    string
	Model      string
	HTTPClient *http.Client
}

// NewMistralClient validates the live Mistral configuration and returns a reusable client.
func NewMistralClient(apiKey, model string) (*MistralClient, error) {
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("llm: Mistral API key is required")
	}
	if strings.TrimSpace(model) == "" {
		model = "mistral-small-latest"
	}

	return &MistralClient{
		APIKey:     apiKey,
		BaseURL:    "https://api.mistral.ai",
		Model:      model,
		HTTPClient: lowLatencyHTTPClient(),
	}, nil
}

// Generate starts one streaming chat-completion request.
//
// The public contract here is intentionally small:
// hand the client a structured request, get back a channel of streamed chunks.
// Everything HTTP-specific stays inside this package so the rest of the system
// can think in terms of streaming text rather than transport details.
func (m *MistralClient) Generate(ctx context.Context, req Request) (<-chan Chunk, error) {
	model := req.Model
	if model == "" {
		model = m.Model
	}
	if model == "" {
		return nil, fmt.Errorf("llm: model is required")
	}

	payload := map[string]any{
		"model":       model,
		"messages":    mistralMessages(req.SystemPrompt, req.Messages),
		"stream":      true,
		"max_tokens":  req.MaxTokens,
		"temperature": req.Temperature,
	}
	if len(req.Tools) > 0 {
		payload["tools"] = req.Tools
		payload["tool_choice"] = "auto"
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("llm: marshal request: %w", err)
	}

	endpoint := strings.TrimRight(m.baseURL(), "/") + "/v1/chat/completions"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("llm: build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+m.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := m.client().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("llm: request failed: %w", err)
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("llm: unexpected status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	out := make(chan Chunk, 16)
	go m.readStream(ctx, resp.Body, out)
	return out, nil
}

// readStream converts server-sent events into the pipeline's `llm.Chunk` type.
//
// SSE arrives as text lines, not as one clean JSON stream. That is why we
// collect `data:` lines into `eventLines`, flush on a blank line, and only then
// decode one logical event.
func (m *MistralClient) readStream(ctx context.Context, body io.ReadCloser, out chan<- Chunk) {
	defer close(out)
	defer body.Close()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var first bool
	var eventLines []string
	toolState := make(map[int]*ToolCall)

	flushEvent := func() bool {
		if len(eventLines) == 0 {
			return true
		}

		payload := strings.Join(eventLines, "\n")
		eventLines = eventLines[:0]
		payload = strings.TrimSpace(payload)
		if payload == "" {
			return true
		}
		if payload == "[DONE]" {
			sendChunk(ctx, out, Chunk{Final: true, Timestamp: time.Now()})
			return false
		}

		// One SSE event may contain either:
		//   - one or more text deltas
		//   - or a finish signal
		// We normalize those provider details into the simpler `Chunk` contract.
		texts, toolDeltas, final := extractMistralEvent([]byte(payload))
		now := time.Now()
		for _, text := range texts {
			if strings.TrimSpace(text) == "" {
				continue
			}
			chunk := Chunk{
				Text:      text,
				First:     !first,
				Final:     false,
				Timestamp: now,
			}
			first = true
			if !sendChunk(ctx, out, chunk) {
				return false
			}
		}
		if mergeToolCallDeltas(toolState, toolDeltas) {
			if !sendChunk(ctx, out, Chunk{
				ToolCalls: snapshotToolCalls(toolState),
				Timestamp: now,
			}) {
				return false
			}
		}

		if final {
			sendChunk(ctx, out, Chunk{Final: true, Timestamp: now})
			return false
		}

		return true
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if !flushEvent() {
				return
			}
			continue
		}

		if !strings.HasPrefix(line, "data:") {
			continue
		}

		eventLines = append(eventLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
	}

	if len(eventLines) > 0 {
		flushEvent()
	}
}

type toolCallDelta struct {
	Index    int
	ID       string
	Type     string
	Function ToolCallFunc
	Full     bool
}

// extractMistralEvent pulls streamed text deltas, tool-call deltas, and finish
// state out of one SSE payload.
func extractMistralEvent(payload []byte) ([]string, []toolCallDelta, bool) {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, nil, false
	}

	choices, _ := event["choices"].([]any)
	var texts []string
	var toolCalls []toolCallDelta
	final := false

	for _, choice := range choices {
		choiceMap, _ := choice.(map[string]any)
		if choiceMap == nil {
			continue
		}

		if finish, ok := choiceMap["finish_reason"].(string); ok && finish != "" {
			final = true
		}

		// Different streaming providers and model versions sometimes place text in
		// different keys (`delta`, `message`, nested `content`, etc).
		// We centralize that variability here so the rest of the client can stay
		// stable even if the wire shape evolves a bit.
		if delta, ok := choiceMap["delta"]; ok {
			texts = append(texts, extractContentText(delta)...)
			toolCalls = append(toolCalls, extractToolCalls(delta, false)...)
		}
		if message, ok := choiceMap["message"]; ok {
			texts = append(texts, extractContentText(message)...)
			toolCalls = append(toolCalls, extractToolCalls(message, true)...)
		}
	}

	return texts, toolCalls, final
}

// extractContentText walks the flexible Mistral content shapes and returns plain text fragments.
func extractContentText(v any) []string {
	switch typed := v.(type) {
	case string:
		if typed == "" {
			return nil
		}
		return []string{typed}
	case map[string]any:
		if content, ok := typed["content"]; ok {
			return extractContentText(content)
		}
		if text, ok := typed["text"]; ok {
			return extractContentText(text)
		}
	case []any:
		var out []string
		for _, item := range typed {
			out = append(out, extractContentText(item)...)
		}
		return out
	}
	return nil
}

func extractToolCalls(v any, full bool) []toolCallDelta {
	msg, _ := v.(map[string]any)
	if msg == nil {
		return nil
	}
	rawCalls, _ := msg["tool_calls"].([]any)
	if len(rawCalls) == 0 {
		return nil
	}

	out := make([]toolCallDelta, 0, len(rawCalls))
	for i, rawCall := range rawCalls {
		callMap, _ := rawCall.(map[string]any)
		if callMap == nil {
			continue
		}

		call := toolCallDelta{
			Index: i,
			ID:    asString(callMap["id"]),
			Type:  asString(callMap["type"]),
			Full:  full,
		}
		if idx, ok := asInt(callMap["index"]); ok {
			call.Index = idx
		}

		functionMap, _ := callMap["function"].(map[string]any)
		if functionMap != nil {
			call.Function = ToolCallFunc{
				Name:      asString(functionMap["name"]),
				Arguments: asString(functionMap["arguments"]),
			}
		}
		out = append(out, call)
	}

	return out
}

func mergeToolCallDeltas(state map[int]*ToolCall, deltas []toolCallDelta) bool {
	if len(deltas) == 0 {
		return false
	}

	changed := false
	for _, delta := range deltas {
		call := state[delta.Index]
		if call == nil {
			call = &ToolCall{}
			state[delta.Index] = call
		}

		if delta.Full {
			next := ToolCall{
				ID:       chooseNonEmpty(delta.ID, call.ID),
				Type:     chooseNonEmpty(delta.Type, call.Type),
				Function: call.Function,
			}
			if next.Type == "" {
				next.Type = "function"
			}
			if delta.Function.Name != "" {
				next.Function.Name = delta.Function.Name
			}
			if delta.Function.Arguments != "" {
				next.Function.Arguments = delta.Function.Arguments
			}
			if *call != next {
				*call = next
				changed = true
			}
			continue
		}

		before := *call
		mergeToolCallField(&call.ID, delta.ID)
		mergeToolCallField(&call.Type, delta.Type)
		if call.Type == "" {
			call.Type = "function"
		}
		mergeToolCallField(&call.Function.Name, delta.Function.Name)
		mergeToolCallField(&call.Function.Arguments, delta.Function.Arguments)
		if before != *call {
			changed = true
		}
	}

	return changed
}

func snapshotToolCalls(state map[int]*ToolCall) []ToolCall {
	if len(state) == 0 {
		return nil
	}

	maxIndex := -1
	for idx := range state {
		if idx > maxIndex {
			maxIndex = idx
		}
	}

	out := make([]ToolCall, 0, len(state))
	for i := 0; i <= maxIndex; i++ {
		call := state[i]
		if call == nil {
			continue
		}
		out = append(out, *call)
	}
	return out
}

func mergeToolCallField(dst *string, fragment string) {
	if fragment == "" {
		return
	}
	if *dst == "" {
		*dst = fragment
		return
	}
	if strings.HasSuffix(*dst, fragment) {
		return
	}
	if strings.HasPrefix(fragment, *dst) {
		*dst = fragment
		return
	}
	*dst += fragment
}

func chooseNonEmpty(primary, fallback string) string {
	if strings.TrimSpace(primary) != "" {
		return primary
	}
	return fallback
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float32:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

// mistralMessages prepends the optional system prompt to the user/assistant history.
func mistralMessages(systemPrompt string, messages []Message) []Message {
	out := make([]Message, 0, len(messages)+1)
	if strings.TrimSpace(systemPrompt) != "" {
		out = append(out, Message{Role: "system", Content: systemPrompt})
	}
	out = append(out, messages...)
	return out
}

// sendChunk forwards one chunk unless the request context has already been canceled.
func sendChunk(ctx context.Context, out chan<- Chunk, chunk Chunk) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- chunk:
		return true
	}
}

// baseURL returns the configured API base or the default public Mistral endpoint.
func (m *MistralClient) baseURL() string {
	if strings.TrimSpace(m.BaseURL) == "" {
		return "https://api.mistral.ai"
	}
	return m.BaseURL
}

// client returns the configured HTTP client or a streaming-safe default with no deadline.
func (m *MistralClient) client() *http.Client {
	if m.HTTPClient == nil {
		m.HTTPClient = lowLatencyHTTPClient()
	}
	return m.HTTPClient
}

// Close releases idle pooled HTTP connections held by the Mistral client.
func (m *MistralClient) Close() error {
	if m.HTTPClient != nil {
		m.HTTPClient.CloseIdleConnections()
	}
	return nil
}

// lowLatencyHTTPClient returns an HTTP client tuned for repeated streaming requests.
//
// The LLM API is still one HTTP request per assistant answer, but that does not
// mean every answer should pay a fresh TCP/TLS setup. Go's Transport owns the
// connection pool. Keeping one Transport alive lets later turns reuse warm
// connections, including HTTP/2 streams when the server supports them.
func lowLatencyHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
}
