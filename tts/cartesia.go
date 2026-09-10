package tts

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// CartesiaEngine streams text-to-speech over Cartesia's websocket API.
//
// The low-latency decision is that this type owns one reusable websocket for
// the live session. A per-turn websocket is simpler, but it pays TCP/TLS/HTTP
// upgrade latency for every assistant response. Cartesia's websocket protocol
// supports multiple generations on one connection, so each assistant turn is
// separated with a unique context_id instead of a new socket.
type CartesiaEngine struct {
	APIKey   string
	Version  string
	BaseURL  string
	ModelID  string
	VoiceID  string
	Language string
	Dialer   *websocket.Dialer
	Header   http.Header

	connectMu   sync.Mutex
	writeMu     sync.Mutex
	conn        *websocket.Conn
	subscribers map[string]chan Chunk
	closed      bool
}

// NewCartesiaEngine validates Cartesia credentials and returns a reusable websocket client.
func NewCartesiaEngine(apiKey string) (*CartesiaEngine, error) {
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("tts: Cartesia API key is required")
	}

	return &CartesiaEngine{
		APIKey:   apiKey,
		Version:  "2025-04-16",
		BaseURL:  "wss://api.cartesia.ai/tts/websocket",
		ModelID:  "sonic-3",
		Language: "en",
		Dialer:   websocket.DefaultDialer,
	}, nil
}

// Synthesize starts one assistant turn on the shared Cartesia websocket.
//
// The websocket stays open across turns; context_id is the per-turn boundary.
// That gives us the latency profile we want: connection setup is paid once at
// session startup/first use, while every later turn can send text immediately.
func (c *CartesiaEngine) Synthesize(ctx context.Context, req Request, text <-chan string) (<-chan Chunk, error) {
	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}

	contextID := contextIDOrDefault(req.ContextID)
	out := make(chan Chunk, 16)

	c.connectMu.Lock()
	if c.closed {
		c.connectMu.Unlock()
		close(out)
		return nil, fmt.Errorf("tts: Cartesia engine is closed")
	}
	c.subscribers[contextID] = out
	c.connectMu.Unlock()

	go func() {
		<-ctx.Done()
		// Cancellation is scoped to the context_id, not the websocket. This is
		// the core low-latency tradeoff: barge-in stops the current utterance,
		// but the next turn keeps the already-open connection.
		_ = c.writeJSON(map[string]any{
			"context_id": contextID,
			"cancel":     true,
		})
		c.unregister(contextID)
	}()

	go c.writePhrases(ctx, contextID, req, text)
	return out, nil
}

// Close closes the session websocket and releases every waiting turn channel.
func (c *CartesiaEngine) Close() error {
	c.connectMu.Lock()
	if c.closed {
		c.connectMu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	subscribers := c.subscribers
	c.subscribers = nil
	c.connectMu.Unlock()

	for contextID, out := range subscribers {
		delete(subscribers, contextID)
		close(out)
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// Warm opens the Cartesia websocket before the first assistant turn needs it.
func (c *CartesiaEngine) Warm(ctx context.Context) error {
	return c.ensureConnected(ctx)
}

// ensureConnected opens the reusable websocket once and starts the single reader loop.
func (c *CartesiaEngine) ensureConnected(ctx context.Context) error {
	c.connectMu.Lock()
	if c.closed {
		c.connectMu.Unlock()
		return fmt.Errorf("tts: Cartesia engine is closed")
	}
	if c.conn != nil {
		c.connectMu.Unlock()
		return nil
	}
	c.connectMu.Unlock()

	url := strings.TrimSpace(c.BaseURL)
	if url == "" {
		url = "wss://api.cartesia.ai/tts/websocket"
	}

	header := http.Header{}
	if c.Header != nil {
		header = c.Header.Clone()
	}
	header.Set("X-API-Key", c.APIKey)
	if version := c.version(); version != "" {
		header.Set("Cartesia-Version", version)
	}

	dialer := c.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	conn, _, err := dialer.DialContext(ctx, url, header)
	if err != nil {
		return fmt.Errorf("tts: connect Cartesia websocket: %w", err)
	}

	c.connectMu.Lock()
	defer c.connectMu.Unlock()
	if c.closed {
		_ = conn.Close()
		return fmt.Errorf("tts: Cartesia engine is closed")
	}
	if c.conn != nil {
		_ = conn.Close()
		return nil
	}
	c.conn = conn
	if c.subscribers == nil {
		c.subscribers = make(map[string]chan Chunk)
	}
	go c.readLoop(conn)
	return nil
}

// writePhrases sends phrase-sized transcript updates into one Cartesia context.
func (c *CartesiaEngine) writePhrases(ctx context.Context, contextID string, req Request, text <-chan string) {
	var pending string
	for {
		select {
		case <-ctx.Done():
			return
		case phrase, ok := <-text:
			if !ok {
				if strings.TrimSpace(pending) == "" {
					c.unregister(contextID)
					return
				}
				if err := c.writeGeneration(req, contextID, pending, false); err != nil {
					log.Printf("tts: failed to finalize Cartesia context: %v", err)
					c.unregister(contextID)
				}
				return
			}

			trimmed := strings.TrimSpace(phrase)
			if trimmed == "" {
				continue
			}
			if strings.TrimSpace(pending) != "" {
				if err := c.writeGeneration(req, contextID, pending, true); err != nil {
					log.Printf("tts: failed to write Cartesia generation request: %v", err)
					c.unregister(contextID)
					return
				}
			}
			pending = trimmed
		}
	}
}

// writeGeneration writes one continuation message for a Cartesia context.
func (c *CartesiaEngine) writeGeneration(req Request, contextID, transcript string, continued bool) error {
	return c.writeJSON(map[string]any{
		"model_id":   chooseString(req.ModelID, c.ModelID),
		"transcript": transcript,
		"voice": map[string]any{
			"mode": "id",
			"id":   chooseString(req.VoiceID, c.VoiceID),
		},
		"language":   chooseString(req.Language, c.Language),
		"context_id": contextID,
		"output_format": map[string]any{
			"container":   "raw",
			"encoding":    "pcm_s16le",
			"sample_rate": audio.SampleRate,
		},
		"continue": continued,
	})
}

// readLoop owns all reads from the shared websocket and routes events by context_id.
func (c *CartesiaEngine) readLoop(conn *websocket.Conn) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("tts: Cartesia read loop panicked: %v", r)
			c.resetConnection(conn)
		}
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			log.Printf("tts: failed to read Cartesia websocket event: %v", err)
			c.resetConnection(conn)
			return
		}

		var envelope struct {
			Type      string `json:"type"`
			Data      string `json:"data"`
			Done      bool   `json:"done"`
			ContextID string `json:"context_id"`
			Message   string `json:"message"`
			Title     string `json:"title"`
			Error     string `json:"error"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			log.Printf("tts: failed to decode Cartesia websocket event: %v", err)
			continue
		}

		switch envelope.Type {
		case "chunk":
			pcm, err := base64.StdEncoding.DecodeString(envelope.Data)
			if err != nil {
				log.Printf("tts: failed to decode Cartesia audio chunk: %v", err)
				continue
			}
			c.sendToSubscriber(envelope.ContextID, Chunk{
				Frame: audio.AudioFrame{
					Data:      pcm,
					Timestamp: time.Now(),
				},
				ContextID: envelope.ContextID,
				Timestamp: time.Now(),
			})
		case "done":
			c.sendToSubscriber(envelope.ContextID, Chunk{
				ContextID: envelope.ContextID,
				Done:      true,
				Timestamp: time.Now(),
			})
			c.unregister(envelope.ContextID)
		case "error":
			log.Printf("tts: Cartesia error: %s %s", envelope.Title, chooseString(envelope.Error, envelope.Message))
			c.unregister(envelope.ContextID)
		case "flush_done", "timestamps", "phoneme_timestamps":
			continue
		}
	}
}

// writeJSON serializes writes because Gorilla allows only one active websocket writer.
func (c *CartesiaEngine) writeJSON(payload any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.connectMu.Lock()
	conn := c.conn
	c.connectMu.Unlock()
	if conn == nil {
		return fmt.Errorf("tts: Cartesia websocket is not connected")
	}
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return conn.WriteJSON(payload)
}

// sendToSubscriber forwards a routed provider chunk to the turn that owns contextID.
func (c *CartesiaEngine) sendToSubscriber(contextID string, chunk Chunk) {
	c.connectMu.Lock()
	out := c.subscribers[contextID]
	c.connectMu.Unlock()
	if out == nil {
		return
	}

	select {
	case out <- chunk:
	default:
		log.Printf("tts: dropping Cartesia chunk for context %q because subscriber is behind", contextID)
	}
}

// unregister removes a context subscriber and closes its output channel exactly once.
func (c *CartesiaEngine) unregister(contextID string) {
	c.connectMu.Lock()
	out := c.subscribers[contextID]
	if out != nil {
		delete(c.subscribers, contextID)
	}
	c.connectMu.Unlock()
	if out != nil {
		close(out)
	}
}

// resetConnection drops the shared socket after failure so the next turn reconnects cleanly.
func (c *CartesiaEngine) resetConnection(conn *websocket.Conn) {
	c.connectMu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	subscribers := c.subscribers
	c.subscribers = make(map[string]chan Chunk)
	c.connectMu.Unlock()

	_ = conn.Close()
	for contextID, out := range subscribers {
		delete(subscribers, contextID)
		close(out)
	}
}

// version returns the configured Cartesia API version or the package default.
func (c *CartesiaEngine) version() string {
	if strings.TrimSpace(c.Version) == "" {
		return "2025-04-16"
	}
	return c.Version
}

// contextIDOrDefault chooses the caller's context ID or generates a unique one for this turn.
func contextIDOrDefault(id string) string {
	if strings.TrimSpace(id) != "" {
		return id
	}
	return fmt.Sprintf("cartesia-%d", time.Now().UnixNano())
}

// chooseString prefers a per-request value and otherwise falls back to the engine default.
func chooseString(primary, fallback string) string {
	if strings.TrimSpace(primary) != "" {
		return primary
	}
	return fallback
}
