package stt

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultAssemblyAIRealtimeURL = "wss://streaming.assemblyai.com/v3/ws"
	assemblyAIMinChunkMS         = 50
	assemblyAITargetChunkMS      = 100
	pcm16BytesPerSample          = 2
)

type AssemblyAIStreamingConfig struct {
	APIKey                       string
	RealtimeURL                  string
	SpeechModel                  string
	SampleRate                   int
	FormatTurns                  bool
	InactivityTimeoutSeconds     int
	VADThreshold                 float64
	EndOfTurnConfidenceThreshold float64
	MinTurnSilenceMS             int
	MaxTurnSilenceMS             int
	KeytermsPrompt               []string
}

// AssemblyAIStreamingSTT uses one websocket per pipeline turn.
//
// That is intentional: the app's STT interface is already turn-scoped, and
// AssemblyAI bills streaming by open websocket session duration. Reusing one
// socket across the whole interview would bill idle think-time between answers.
type AssemblyAIStreamingSTT struct {
	realtimeURL string
	apiKey      string
	dialer      *websocket.Dialer
	query       url.Values
}

type assemblyAIBeginEvent struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type assemblyAITerminationEvent struct {
	Type                 string  `json:"type"`
	AudioDurationSeconds float64 `json:"audio_duration_seconds"`
}

type assemblyAITurnWord struct {
	Start       int64   `json:"start"`
	End         int64   `json:"end"`
	Text        string  `json:"text"`
	Confidence  float64 `json:"confidence"`
	WordIsFinal bool    `json:"word_is_final"`
}

type assemblyAITurnEvent struct {
	Type                string               `json:"type"`
	TurnOrder           int                  `json:"turn_order"`
	EndOfTurn           bool                 `json:"end_of_turn"`
	EndOfTurnConfidence float64              `json:"end_of_turn_confidence"`
	Transcript          string               `json:"transcript"`
	Utterance           string               `json:"utterance"`
	Words               []assemblyAITurnWord `json:"words"`
}

type assemblyAIErrorEnvelope struct {
	Error any    `json:"error"`
	Type  string `json:"type"`
	Code  any    `json:"code"`
}

type assemblyAITranscriptState struct {
	finalizedByOrder map[int]string
	currentOrder     int
	currentText      string
	hasCurrent       bool
	lastConfidence   float64
	lastStartMS      int64
	lastEndMS        int64
	lastTimestamp    time.Time
}

func NewAssemblyAIStreamingSTT(cfg AssemblyAIStreamingConfig) (*AssemblyAIStreamingSTT, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("stt: AssemblyAI API key is required")
	}
	if strings.TrimSpace(cfg.SpeechModel) == "" {
		return nil, fmt.Errorf("stt: AssemblyAI speech model is required")
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = audio.SampleRate
	}
	if strings.TrimSpace(cfg.RealtimeURL) == "" {
		cfg.RealtimeURL = defaultAssemblyAIRealtimeURL
	}

	query := url.Values{}
	query.Set("speech_model", strings.TrimSpace(cfg.SpeechModel))
	query.Set("sample_rate", fmt.Sprintf("%d", cfg.SampleRate))
	query.Set("encoding", "pcm_s16le")
	query.Set("format_turns", fmt.Sprintf("%t", cfg.FormatTurns))
	if cfg.InactivityTimeoutSeconds > 0 {
		query.Set("inactivity_timeout", fmt.Sprintf("%d", cfg.InactivityTimeoutSeconds))
	}
	if cfg.VADThreshold > 0 {
		query.Set("vad_threshold", trimFloat(cfg.VADThreshold))
	}
	if cfg.EndOfTurnConfidenceThreshold >= 0 {
		query.Set("end_of_turn_confidence_threshold", trimFloat(cfg.EndOfTurnConfidenceThreshold))
	}
	if cfg.MinTurnSilenceMS > 0 {
		query.Set("min_turn_silence", fmt.Sprintf("%d", cfg.MinTurnSilenceMS))
	}
	if cfg.MaxTurnSilenceMS > 0 {
		query.Set("max_turn_silence", fmt.Sprintf("%d", cfg.MaxTurnSilenceMS))
	}
	for _, keyterm := range normalizeAssemblyAIKeyterms(cfg.KeytermsPrompt) {
		query.Add("keyterms_prompt", keyterm)
	}

	return &AssemblyAIStreamingSTT{
		realtimeURL: strings.TrimRight(strings.TrimSpace(cfg.RealtimeURL), "/"),
		apiKey:      strings.TrimSpace(cfg.APIKey),
		dialer:      websocket.DefaultDialer,
		query:       query,
	}, nil
}

func (a *AssemblyAIStreamingSTT) Transcribe(ctx context.Context, audioStream <-chan *audio.AudioFrame) (<-chan Transcript, error) {
	out := make(chan Transcript, 16)
	go a.runTurn(ctx, audioStream, out)
	return out, nil
}

func (a *AssemblyAIStreamingSTT) runTurn(ctx context.Context, audioStream <-chan *audio.AudioFrame, out chan<- Transcript) {
	defer close(out)

	conn, err := a.dial(ctx)
	if err != nil {
		sendSTTError(out, fmt.Errorf("stt: connect to AssemblyAI streaming: %w", err))
		return
	}
	defer conn.Close()

	beginCtx, cancelBegin := context.WithTimeout(ctx, 5*time.Second)
	if err := a.awaitBegin(beginCtx, conn); err != nil {
		cancelBegin()
		sendSTTError(out, err)
		return
	}
	cancelBegin()

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- a.streamAudio(ctx, conn, audioStream)
	}()

	state := newAssemblyAITranscriptState()
	audioSendFinished := false

	for {
		if err := conn.SetReadDeadline(time.Now().Add(8 * time.Second)); err != nil {
			sendSTTError(out, fmt.Errorf("stt: set AssemblyAI read deadline: %w", err))
			return
		}

		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			select {
			case sendErr := <-sendDone:
				if sendErr != nil {
					sendSTTError(out, sendErr)
					return
				}
				audioSendFinished = true
			default:
			}
			if ctx.Err() != nil {
				return
			}
			if audioSendFinished && state.hasAnyText() {
				out <- state.finalTranscript()
				return
			}
			sendSTTError(out, fmt.Errorf("stt: read AssemblyAI websocket message: %w", err))
			return
		}
		if messageType != websocket.TextMessage {
			continue
		}

		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(payload, &envelope); err != nil {
			continue
		}

		switch envelope.Type {
		case "Turn":
			var turn assemblyAITurnEvent
			if err := json.Unmarshal(payload, &turn); err != nil {
				continue
			}
			transcript, ok := state.applyTurn(turn)
			if ok {
				select {
				case <-ctx.Done():
					return
				case out <- transcript:
				}
			}
		case "Termination":
			var event assemblyAITerminationEvent
			_ = json.Unmarshal(payload, &event)
			select {
			case sendErr := <-sendDone:
				if sendErr != nil {
					sendSTTError(out, sendErr)
					return
				}
			default:
			}
			if state.hasAnyText() {
				out <- state.finalTranscript()
			}
			return
		case "Begin":
			continue
		default:
			var errEnvelope assemblyAIErrorEnvelope
			if err := json.Unmarshal(payload, &errEnvelope); err == nil && errEnvelope.Error != nil {
				sendSTTError(out, fmt.Errorf("stt: AssemblyAI streaming error: %v", errEnvelope.Error))
				return
			}
		}
	}
}

func (a *AssemblyAIStreamingSTT) dial(ctx context.Context) (*websocket.Conn, error) {
	target, err := url.Parse(a.realtimeURL)
	if err != nil {
		return nil, err
	}
	target.RawQuery = a.query.Encode()

	header := http.Header{}
	header.Set("Authorization", a.apiKey)
	conn, _, err := a.dialer.DialContext(ctx, target.String(), header)
	return conn, err
}

func (a *AssemblyAIStreamingSTT) awaitBegin(ctx context.Context, conn *websocket.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("stt: set AssemblyAI handshake deadline: %w", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("stt: wait for AssemblyAI begin event: %w", err)
	}

	var begin assemblyAIBeginEvent
	if err := json.Unmarshal(payload, &begin); err != nil {
		return fmt.Errorf("stt: decode AssemblyAI begin event: %w", err)
	}
	if begin.Type != "Begin" {
		return fmt.Errorf("stt: unexpected AssemblyAI handshake event %q", begin.Type)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return conn.SetReadDeadline(time.Time{})
}

func (a *AssemblyAIStreamingSTT) streamAudio(ctx context.Context, conn *websocket.Conn, audioStream <-chan *audio.AudioFrame) error {
	pending := make([]byte, 0, audio.FrameSize*5)
	targetChunkBytes := chunkBytesForDuration(assemblyAITargetChunkMS)
	minChunkBytes := chunkBytesForDuration(assemblyAIMinChunkMS)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame, ok := <-audioStream:
			if !ok {
				if len(pending) > 0 {
					if len(pending) < minChunkBytes {
						padding := make([]byte, minChunkBytes-len(pending))
						pending = append(pending, padding...)
					}
					if err := writeAssemblyAIBinary(conn, pending); err != nil {
						return err
					}
				}
				if err := writeAssemblyAIJSON(conn, map[string]any{"type": "ForceEndpoint"}); err != nil {
					return err
				}
				return writeAssemblyAIJSON(conn, map[string]any{"type": "Terminate"})
			}
			if frame == nil || len(frame.Data) == 0 {
				continue
			}
			pending = append(pending, frame.Data...)
			for len(pending) >= targetChunkBytes {
				chunk := append([]byte(nil), pending[:targetChunkBytes]...)
				pending = pending[targetChunkBytes:]
				if err := writeAssemblyAIBinary(conn, chunk); err != nil {
					return err
				}
			}
		}
	}
}

func writeAssemblyAIBinary(conn *websocket.Conn, chunk []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("stt: set AssemblyAI write deadline: %w", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, chunk); err != nil {
		return fmt.Errorf("stt: write AssemblyAI audio chunk: %w", err)
	}
	return nil
}

func writeAssemblyAIJSON(conn *websocket.Conn, payload any) error {
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("stt: set AssemblyAI write deadline: %w", err)
	}
	if err := conn.WriteJSON(payload); err != nil {
		return fmt.Errorf("stt: write AssemblyAI control message: %w", err)
	}
	return nil
}

func newAssemblyAITranscriptState() *assemblyAITranscriptState {
	return &assemblyAITranscriptState{
		finalizedByOrder: make(map[int]string),
		lastTimestamp:    time.Now(),
	}
}

func (s *assemblyAITranscriptState) applyTurn(turn assemblyAITurnEvent) (Transcript, bool) {
	text := strings.TrimSpace(turn.Transcript)
	if text == "" && !turn.EndOfTurn {
		return Transcript{}, false
	}

	s.lastConfidence = averageWordConfidence(turn.Words)
	s.lastStartMS, s.lastEndMS = wordTiming(turn.Words)
	s.lastTimestamp = time.Now()

	if turn.EndOfTurn {
		if text != "" {
			s.finalizedByOrder[turn.TurnOrder] = text
		}
		if s.hasCurrent && s.currentOrder == turn.TurnOrder {
			s.hasCurrent = false
			s.currentText = ""
		}
	} else {
		s.currentOrder = turn.TurnOrder
		s.currentText = text
		s.hasCurrent = true
	}

	combined := s.combinedText()
	if combined == "" {
		return Transcript{}, false
	}

	return Transcript{
		Text:       combined,
		Confidence: s.lastConfidence,
		Timestamp:  s.lastTimestamp,
		IsFinal:    false,
		StartMS:    s.lastStartMS,
		EndMS:      s.lastEndMS,
	}, true
}

func (s *assemblyAITranscriptState) finalTranscript() Transcript {
	return Transcript{
		Text:       s.combinedText(),
		Confidence: s.lastConfidence,
		Timestamp:  s.lastTimestamp,
		IsFinal:    true,
		StartMS:    s.lastStartMS,
		EndMS:      s.lastEndMS,
	}
}

func (s *assemblyAITranscriptState) hasAnyText() bool {
	return s.combinedText() != ""
}

func (s *assemblyAITranscriptState) combinedText() string {
	orders := make([]int, 0, len(s.finalizedByOrder))
	for order := range s.finalizedByOrder {
		orders = append(orders, order)
	}
	sort.Ints(orders)

	parts := make([]string, 0, len(orders)+1)
	for _, order := range orders {
		if text := strings.TrimSpace(s.finalizedByOrder[order]); text != "" {
			parts = append(parts, text)
		}
	}
	if s.hasCurrent {
		if text := strings.TrimSpace(s.currentText); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func normalizeAssemblyAIKeyterms(keyterms []string) []string {
	seen := make(map[string]struct{}, len(keyterms))
	normalized := make([]string, 0, len(keyterms))
	for _, keyterm := range keyterms {
		cleaned := strings.TrimSpace(keyterm)
		if cleaned == "" || len(cleaned) > 50 {
			continue
		}
		lowered := strings.ToLower(cleaned)
		if _, exists := seen[lowered]; exists {
			continue
		}
		seen[lowered] = struct{}{}
		normalized = append(normalized, cleaned)
		if len(normalized) == 100 {
			break
		}
	}
	return normalized
}

func averageWordConfidence(words []assemblyAITurnWord) float64 {
	if len(words) == 0 {
		return 0
	}
	var total float64
	var count int
	for _, word := range words {
		if word.Confidence <= 0 {
			continue
		}
		total += word.Confidence
		count++
	}
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func wordTiming(words []assemblyAITurnWord) (int64, int64) {
	if len(words) == 0 {
		return 0, 0
	}
	return words[0].Start, words[len(words)-1].End
}

func chunkBytesForDuration(durationMS int) int {
	samples := audio.SampleRate * durationMS / 1000
	return samples * pcm16BytesPerSample
}

func trimFloat(value float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.4f", value), "0"), ".")
}

func sendSTTError(out chan<- Transcript, err error) {
	select {
	case out <- Transcript{Err: err, Timestamp: time.Now()}:
	default:
	}
}
