// stt/voxtral.go

package stt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MistralVoxtralSTT is a realtime STT client backed by Mistral's websocket API.
//
// The low-latency decision is to keep the websocket open across normal turns.
// Creating a new websocket for every utterance adds avoidable handshake latency.
// The one caveat is cancellation: our current raw event format has no turn ID,
// so if a turn is canceled before the provider finishes, we close the socket to
// prevent stale transcript events from being routed into the next turn.
type MistralVoxtralSTT struct {
	model         string
	realtimeURL   string
	requestHeader http.Header
	dialer        *websocket.Dialer

	mu      sync.Mutex
	writeMu sync.Mutex
	conn    *websocket.Conn
	current *mistralTurn
	closed  bool
}

// mistralTurn is the currently active STT turn on the shared websocket.
type mistralTurn struct {
	out      chan Transcript
	fullText string
}

// AudioAppendEvent is the client event we send for each PCM audio chunk.
type AudioAppendEvent struct {
	Type  string `json:"type"`
	Audio string `json:"audio"` // Base64 encoded pcm_s16le bytes
}

// audioCommitEvent tells the provider that the current buffered user utterance is complete.
type audioCommitEvent struct {
	Type string `json:"type"`
}

// mistralRealtimeEventEnvelope is the minimal decoded shape needed to branch on event type.
type mistralRealtimeEventEnvelope struct {
	Type string `json:"type"`
}

// mistralRealtimeTextDeltaEvent represents one incremental transcript update from Mistral.
type mistralRealtimeTextDeltaEvent struct {
	Type        string  `json:"type"`
	Text        string  `json:"text"`
	Confidence  float64 `json:"confidence,omitempty"`
	StartMS     int64   `json:"start_ms,omitempty"`
	EndMS       int64   `json:"end_ms,omitempty"`
	Timestamp   string  `json:"timestamp,omitempty"`
	TimestampMS int64   `json:"timestamp_ms,omitempty"`
}

// mistralRealtimeDoneEvent represents the provider event that marks one transcript as final.
type mistralRealtimeDoneEvent struct {
	Type        string  `json:"type"`
	Confidence  float64 `json:"confidence,omitempty"`
	StartMS     int64   `json:"start_ms,omitempty"`
	EndMS       int64   `json:"end_ms,omitempty"`
	Timestamp   string  `json:"timestamp,omitempty"`
	TimestampMS int64   `json:"timestamp_ms,omitempty"`
}

// mistralRealtimeErrorEvent captures provider-side websocket errors in a small decoded form.
type mistralRealtimeErrorEvent struct {
	Type  string `json:"type"`
	Error struct {
		Message string `json:"message"`
		Code    string `json:"code,omitempty"`
		Type    string `json:"type,omitempty"`
	} `json:"error"`
}

const (
	mistralEventSessionCreatedV1 = "transcription_session.created"
	mistralEventSessionCreatedV2 = "realtime.transcription_session.created"
	mistralEventSessionCreatedV3 = "realtime_transcription_session.created"

	mistralEventTextDeltaV1 = "transcription_stream.text_delta"
	mistralEventTextDeltaV2 = "realtime.transcription_stream.text_delta"
	mistralEventTextDeltaV3 = "realtime_transcription_stream.text_delta"

	mistralEventDoneV1 = "transcription_stream.done"
	mistralEventDoneV2 = "realtime.transcription_stream.done"
	mistralEventDoneV3 = "realtime_transcription_stream.done"

	mistralEventErrorV1 = "transcription.error"
	mistralEventErrorV2 = "realtime.transcription.error"
	mistralEventErrorV3 = "realtime_transcription_error"
)

// NewMistralVoxtralSTT validates the config and prepares a reusable client.
//
// The websocket URL is passed in explicitly because different deployments may
// place model selection in the URL, headers, or query string.
func NewMistralVoxtralSTT(model, realtimeURL string, requestHeader http.Header) (*MistralVoxtralSTT, error) {
	if model == "" {
		return nil, fmt.Errorf("stt: model is required")
	}

	if realtimeURL == "" {
		return nil, fmt.Errorf("stt: realtime websocket URL is required")
	}

	var headerCopy http.Header
	if requestHeader != nil {
		headerCopy = requestHeader.Clone()
	}

	return &MistralVoxtralSTT{
		model:         model,
		realtimeURL:   realtimeURL,
		requestHeader: headerCopy,
		dialer:        websocket.DefaultDialer,
	}, nil
}

// Transcribe starts one user utterance on the reusable realtime websocket.
//
// Normal turns share the same websocket to avoid reconnect latency. Only one
// STT turn is active at a time because the current wire events do not carry a
// turn identifier. If a new turn interrupts an unfinished one, we reset the
// socket so old provider events cannot contaminate the new transcript.
func (mv *MistralVoxtralSTT) Transcribe(ctx context.Context, audioStream <-chan *audio.AudioFrame) (<-chan Transcript, error) {
	if err := mv.ensureConnected(ctx); err != nil {
		return nil, err
	}

	out := make(chan Transcript, 10)
	turn := &mistralTurn{out: out}

	mv.mu.Lock()
	if mv.closed {
		mv.mu.Unlock()
		close(out)
		return nil, fmt.Errorf("stt: Mistral Voxtral client is closed")
	}
	if mv.current != nil {
		// There is no context_id in the STT stream, so overlapping turns cannot
		// be safely demultiplexed. Resetting trades one reconnect for correctness
		// only on the interruption path.
		conn := mv.conn
		mv.mu.Unlock()
		mv.resetConnection(conn)
		if err := mv.ensureConnected(ctx); err != nil {
			close(out)
			return nil, err
		}
		mv.mu.Lock()
	}
	mv.current = turn
	conn := mv.conn
	mv.mu.Unlock()

	go func() {
		committed := mv.sendToWebSocket(ctx, conn, audioStream)
		if !committed {
			mv.resetConnection(conn)
		}
	}()

	return out, nil
}

// Close shuts down the reusable STT websocket and the active transcript channel.
func (mv *MistralVoxtralSTT) Close() error {
	mv.mu.Lock()
	if mv.closed {
		mv.mu.Unlock()
		return nil
	}
	mv.closed = true
	conn := mv.conn
	mv.conn = nil
	turn := mv.current
	mv.current = nil
	mv.mu.Unlock()

	if turn != nil {
		close(turn.out)
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// Warm opens the realtime STT websocket before the first user utterance.
func (mv *MistralVoxtralSTT) Warm(ctx context.Context) error {
	return mv.ensureConnected(ctx)
}

// ensureConnected opens the reusable websocket and starts the single reader loop.
func (mv *MistralVoxtralSTT) ensureConnected(ctx context.Context) error {
	mv.mu.Lock()
	if mv.closed {
		mv.mu.Unlock()
		return fmt.Errorf("stt: Mistral Voxtral client is closed")
	}
	if mv.conn != nil {
		mv.mu.Unlock()
		return nil
	}
	mv.mu.Unlock()

	if mv.realtimeURL == "" {
		return fmt.Errorf("stt: realtime websocket URL is required")
	}
	dialer := mv.dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	conn, _, err := dialer.DialContext(ctx, mv.realtimeURL, mv.requestHeader)
	if err != nil {
		return fmt.Errorf("stt: failed to connect to Mistral realtime STT for model %q: %w", mv.model, err)
	}

	mv.mu.Lock()
	defer mv.mu.Unlock()
	if mv.closed {
		_ = conn.Close()
		return fmt.Errorf("stt: Mistral Voxtral client is closed")
	}
	if mv.conn != nil {
		_ = conn.Close()
		return nil
	}
	mv.conn = conn
	go mv.readLoop(conn)
	return nil
}

// sendToWebSocket forwards PCM frames and commits the provider input buffer at turn end.
func (mv *MistralVoxtralSTT) sendToWebSocket(ctx context.Context, conn *websocket.Conn, audioStream <-chan *audio.AudioFrame) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case frame, ok := <-audioStream:
			if !ok {
				// Committing tells the server: "the user utterance is complete;
				// finalize the audio buffered so far." We keep the websocket open
				// afterward so the next normal turn avoids a reconnect.
				if err := mv.writeJSON(conn, &audioCommitEvent{Type: "input_audio_buffer.commit"}); err != nil {
					log.Printf("stt: failed to commit websocket audio buffer for model %q: %v", mv.model, err)
					return false
				}
				return true
			}

			if frame == nil || len(frame.Data) == 0 {
				continue
			}
			if len(frame.Data)%2 != 0 {
				log.Printf("stt: skipping malformed PCM16 frame with odd byte length %d", len(frame.Data))
				continue
			}

			event := AudioAppendEvent{
				Type:  "input_audio_buffer.append",
				Audio: base64.StdEncoding.EncodeToString(frame.Data),
			}
			if err := mv.writeJSON(conn, &event); err != nil {
				log.Printf("stt: failed to send audio frame to websocket for model %q: %v", mv.model, err)
				return false
			}
		}
	}
}

// readLoop owns all reads from the shared STT websocket and routes them to the active turn.
func (mv *MistralVoxtralSTT) readLoop(conn *websocket.Conn) {
	for {
		if err := conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			log.Printf("stt: failed to set websocket read deadline: %v", err)
			mv.resetConnection(conn)
			return
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				mv.resetConnection(conn)
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			log.Printf("stt: failed to read websocket message: %v", err)
			mv.resetConnection(conn)
			return
		}

		mv.handleRealtimePayload(data)
	}
}

// handleRealtimePayload decodes one provider event and forwards it to the active turn.
func (mv *MistralVoxtralSTT) handleRealtimePayload(data []byte) {
	mv.mu.Lock()
	turn := mv.current
	fullText := ""
	if turn != nil {
		fullText = turn.fullText
	}
	mv.mu.Unlock()
	if turn == nil {
		return
	}

	event, action, ok := decodeMistralRealtimeEvent(data, fullText)
	if !ok {
		if action == mistralRealtimeActionError {
			mv.finishTurn(turn)
		}
		return
	}

	switch action {
	case mistralRealtimeActionIgnore:
		return
	case mistralRealtimeActionDelta:
		mv.mu.Lock()
		if mv.current == turn {
			turn.fullText = event.Text
		}
		mv.mu.Unlock()
	case mistralRealtimeActionDone:
		if event.Text == "" {
			event.Text = fullText
		}
	case mistralRealtimeActionError:
		mv.finishTurn(turn)
		return
	}

	if event.Text == "" {
		return
	}
	mv.sendTranscript(turn, event)
	if action == mistralRealtimeActionDone {
		mv.finishTurn(turn)
	}
}

// sendTranscript forwards one transcript update unless the turn has already been replaced.
func (mv *MistralVoxtralSTT) sendTranscript(turn *mistralTurn, transcript Transcript) {
	mv.mu.Lock()
	active := mv.current == turn
	out := turn.out
	mv.mu.Unlock()
	if !active {
		return
	}

	if transcript.IsFinal {
		out <- transcript
		return
	}
	select {
	case out <- transcript:
	default:
		log.Printf("stt: dropping transcript update because subscriber is behind")
	}
}

// finishTurn clears the active turn and closes its transcript channel.
func (mv *MistralVoxtralSTT) finishTurn(turn *mistralTurn) {
	mv.mu.Lock()
	if mv.current != turn {
		mv.mu.Unlock()
		return
	}
	mv.current = nil
	mv.mu.Unlock()
	close(turn.out)
}

// resetConnection drops the websocket and closes the active turn after failure/cancellation.
func (mv *MistralVoxtralSTT) resetConnection(conn *websocket.Conn) {
	if conn == nil {
		return
	}

	mv.mu.Lock()
	if mv.conn == conn {
		mv.conn = nil
	}
	turn := mv.current
	mv.current = nil
	mv.mu.Unlock()

	_ = conn.Close()
	if turn != nil {
		close(turn.out)
	}
}

// writeJSON serializes websocket writes and applies a short write deadline.
func (mv *MistralVoxtralSTT) writeJSON(conn *websocket.Conn, payload any) error {
	mv.writeMu.Lock()
	defer mv.writeMu.Unlock()

	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return conn.WriteJSON(payload)
}

// mistralRealtimeAction describes what the session reader should do with a decoded event.
type mistralRealtimeAction int

const (
	mistralRealtimeActionIgnore mistralRealtimeAction = iota
	mistralRealtimeActionDelta
	mistralRealtimeActionDone
	mistralRealtimeActionError
)

// decodeMistralRealtimeEvent maps raw websocket payloads into the pipeline's
// Transcript type.
//
// The exact raw `type` strings are inferred from Mistral's documented realtime
// event model. Once you capture production payloads, this switch is the one
// place to tighten to the exact wire contract.
func decodeMistralRealtimeEvent(data []byte, fullText string) (Transcript, mistralRealtimeAction, bool) {
	var envelope mistralRealtimeEventEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		log.Printf("stt: failed to decode Mistral realtime event: %v", err)
		return Transcript{}, mistralRealtimeActionIgnore, false
	}

	switch envelope.Type {
	case mistralEventSessionCreatedV1, mistralEventSessionCreatedV2, mistralEventSessionCreatedV3:
		return Transcript{}, mistralRealtimeActionIgnore, false

	case mistralEventTextDeltaV1, mistralEventTextDeltaV2, mistralEventTextDeltaV3:
		var event mistralRealtimeTextDeltaEvent
		if err := json.Unmarshal(data, &event); err != nil {
			log.Printf("stt: failed to decode Mistral text delta event: %v", err)
			return Transcript{}, mistralRealtimeActionIgnore, false
		}

		// The pipeline contract uses cumulative transcript text because the
		// stabilizer compares "previous full text" to "current full text".
		// That is why we append the new delta onto the previously accumulated text.
		nextText := fullText + event.Text
		return Transcript{
			Text:       nextText,
			Confidence: event.Confidence,
			Timestamp:  mistralEventTimestamp(event.TimestampMS, event.Timestamp),
			IsFinal:    false,
			StartMS:    event.StartMS,
			EndMS:      event.EndMS,
		}, mistralRealtimeActionDelta, true

	case mistralEventDoneV1, mistralEventDoneV2, mistralEventDoneV3:
		var event mistralRealtimeDoneEvent
		if err := json.Unmarshal(data, &event); err != nil {
			log.Printf("stt: failed to decode Mistral done event: %v", err)
			return Transcript{}, mistralRealtimeActionIgnore, false
		}

		return Transcript{
			Text:       fullText,
			Confidence: event.Confidence,
			Timestamp:  mistralEventTimestamp(event.TimestampMS, event.Timestamp),
			IsFinal:    true,
			StartMS:    event.StartMS,
			EndMS:      event.EndMS,
		}, mistralRealtimeActionDone, true

	case mistralEventErrorV1, mistralEventErrorV2, mistralEventErrorV3:
		var event mistralRealtimeErrorEvent
		if err := json.Unmarshal(data, &event); err != nil {
			log.Printf("stt: failed to decode Mistral error event: %v", err)
			return Transcript{}, mistralRealtimeActionError, false
		}

		log.Printf("stt: Mistral realtime transcription error: %s", event.Error.Message)
		return Transcript{}, mistralRealtimeActionError, false

	default:
		return Transcript{}, mistralRealtimeActionIgnore, false
	}
}

// mistralEventTimestamp prefers provider timestamps and falls back to local time when absent.
func mistralEventTimestamp(timestampMS int64, timestamp string) time.Time {
	if timestampMS > 0 {
		return time.UnixMilli(timestampMS)
	}

	if timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, timestamp); err == nil {
			return parsed
		}
	}

	return time.Now()
}
