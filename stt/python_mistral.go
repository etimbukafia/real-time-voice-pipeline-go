package stt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	pythonMistralCmdStart  = 0x01
	pythonMistralCmdFrame  = 0x02
	pythonMistralCmdEnd    = 0x03
	pythonMistralCmdCancel = 0x04
	pythonMistralCmdClose  = 0x05

	pythonMistralEventPartial = 0x11
	pythonMistralEventFinal   = 0x12
	pythonMistralEventError   = 0x13
)

type PythonMistralRealtimeConfig struct {
	PythonExe              string
	APIKey                 string
	Model                  string
	ServerURL              string
	TargetStreamingDelayMS int
}

type PythonMistralRealtimeSTT struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	stderr *bytes.Buffer

	mu        sync.Mutex
	current   *pythonMistralTurn
	closed    bool
	workerErr error
}

type pythonMistralTurn struct {
	out chan Transcript
}

type pythonMistralTranscriptEvent struct {
	Text       string  `json:"text"`
	Confidence float64 `json:"confidence"`
	Timestamp  string  `json:"timestamp"`
	IsFinal    bool    `json:"is_final"`
	StartMS    int64   `json:"start_ms"`
	EndMS      int64   `json:"end_ms"`
}

type pythonMistralErrorEvent struct {
	Message string `json:"message"`
}

func NewPythonMistralRealtimeSTT(cfg PythonMistralRealtimeConfig) (*PythonMistralRealtimeSTT, error) {
	if strings.TrimSpace(cfg.PythonExe) == "" {
		cfg.PythonExe = "python"
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("stt: Mistral API key is required")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, fmt.Errorf("stt: model is required")
	}

	scriptPath, err := pythonMistralWorkerPath()
	if err != nil {
		return nil, err
	}

	args := []string{
		"-u",
		scriptPath,
		"--api-key", cfg.APIKey,
		"--model", cfg.Model,
		"--server-url", normalizeMistralServerURL(cfg.ServerURL),
	}
	if cfg.TargetStreamingDelayMS > 0 {
		args = append(args, "--target-delay-ms", strconv.Itoa(cfg.TargetStreamingDelayMS))
	}

	cmd := exec.Command(cfg.PythonExe, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stt: create Python stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stt: create Python stdout pipe: %w", err)
	}
	stderrBuf := &bytes.Buffer{}
	cmd.Stderr = stderrBuf

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("stt: start Python Mistral worker: %w", err)
	}

	client := &PythonMistralRealtimeSTT{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdoutPipe),
		stderr: stderrBuf,
	}

	ready := make([]byte, 4)
	if _, err := io.ReadFull(client.stdout, ready); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("stt: Python Mistral worker failed to initialize: %w; stderr: %s", err, stderrBuf.String())
	}
	if string(ready) != "MRDY" {
		_ = client.Close()
		return nil, fmt.Errorf("stt: Python Mistral worker sent unexpected handshake %q; stderr: %s", string(ready), stderrBuf.String())
	}

	go client.readLoop()
	return client, nil
}

func (p *PythonMistralRealtimeSTT) Transcribe(ctx context.Context, audioStream <-chan *audio.AudioFrame) (<-chan Transcript, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("stt: Python Mistral client is closed")
	}
	if p.workerErr != nil {
		return nil, p.workerErr
	}
	if p.current != nil {
		p.cancelCurrentLocked()
	}

	out := make(chan Transcript, 16)
	turn := &pythonMistralTurn{out: out}
	p.current = turn
	if err := p.writeCommandLocked(pythonMistralCmdStart, nil); err != nil {
		p.current = nil
		close(out)
		return nil, err
	}

	go p.streamTurn(ctx, turn, audioStream)
	return out, nil
}

func (p *PythonMistralRealtimeSTT) Warm(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("stt: Python Mistral client is closed")
	}
	return p.workerErr
}

func (p *PythonMistralRealtimeSTT) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.cancelCurrentLocked()
	_ = p.writeCommandLocked(pythonMistralCmdClose, nil)
	if p.stdin != nil {
		_ = p.stdin.Close()
		p.stdin = nil
	}
	cmd := p.cmd
	p.cmd = nil
	p.mu.Unlock()

	if cmd == nil {
		return nil
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("stt: wait for Python Mistral worker: %w; stderr: %s", err, p.stderr.String())
	}
	return nil
}

func (p *PythonMistralRealtimeSTT) streamTurn(ctx context.Context, turn *pythonMistralTurn, audioStream <-chan *audio.AudioFrame) {
	for {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			if p.current == turn {
				p.cancelCurrentLocked()
			}
			p.mu.Unlock()
			return
		case frame, ok := <-audioStream:
			if !ok {
				p.mu.Lock()
				if p.current == turn && !p.closed && p.workerErr == nil {
					_ = p.writeCommandLocked(pythonMistralCmdEnd, nil)
				}
				p.mu.Unlock()
				return
			}
			if frame == nil || len(frame.Data) == 0 {
				continue
			}
			p.mu.Lock()
			if p.current != turn || p.closed || p.workerErr != nil {
				p.mu.Unlock()
				return
			}
			err := p.writeCommandLocked(pythonMistralCmdFrame, frame.Data)
			p.mu.Unlock()
			if err != nil {
				p.failWorker(err)
				return
			}
		}
	}
}

func (p *PythonMistralRealtimeSTT) readLoop() {
	for {
		eventType, err := p.stdout.ReadByte()
		if err != nil {
			if p.isClosed() {
				return
			}
			p.failWorker(fmt.Errorf("stt: read Python Mistral event type: %w; stderr: %s", err, p.stderr.String()))
			return
		}

		var lenBuf [4]byte
		if _, err := io.ReadFull(p.stdout, lenBuf[:]); err != nil {
			p.failWorker(fmt.Errorf("stt: read Python Mistral event length: %w; stderr: %s", err, p.stderr.String()))
			return
		}
		size := binary.LittleEndian.Uint32(lenBuf[:])
		payload := make([]byte, size)
		if _, err := io.ReadFull(p.stdout, payload); err != nil {
			p.failWorker(fmt.Errorf("stt: read Python Mistral event payload: %w; stderr: %s", err, p.stderr.String()))
			return
		}

		switch eventType {
		case pythonMistralEventPartial, pythonMistralEventFinal:
			var event pythonMistralTranscriptEvent
			if err := json.Unmarshal(payload, &event); err != nil {
				continue
			}
			p.deliverTranscript(Transcript{
				Text:       event.Text,
				Confidence: event.Confidence,
				Timestamp:  parsePythonMistralTimestamp(event.Timestamp),
				IsFinal:    eventType == pythonMistralEventFinal || event.IsFinal,
				StartMS:    event.StartMS,
				EndMS:      event.EndMS,
			})
		case pythonMistralEventError:
			var event pythonMistralErrorEvent
			if err := json.Unmarshal(payload, &event); err != nil {
				p.failCurrentTurn(fmt.Errorf("stt: transcription worker error"))
				continue
			}
			p.failCurrentTurn(fmt.Errorf("stt: %s", strings.TrimSpace(event.Message)))
		default:
			p.failCurrentTurn(fmt.Errorf("stt: transcription worker sent unknown event type %d", eventType))
		}
	}
}

func (p *PythonMistralRealtimeSTT) deliverTranscript(transcript Transcript) {
	p.mu.Lock()
	turn := p.current
	if transcript.IsFinal {
		p.current = nil
	}
	p.mu.Unlock()
	if turn == nil {
		return
	}

	select {
	case turn.out <- transcript:
	default:
	}
	if transcript.IsFinal {
		close(turn.out)
	}
}

func (p *PythonMistralRealtimeSTT) closeCurrentTurn() {
	p.mu.Lock()
	turn := p.current
	p.current = nil
	p.mu.Unlock()
	if turn != nil {
		close(turn.out)
	}
}

func (p *PythonMistralRealtimeSTT) failCurrentTurn(err error) {
	p.mu.Lock()
	turn := p.current
	p.current = nil
	p.mu.Unlock()
	if turn == nil {
		return
	}
	select {
	case turn.out <- Transcript{Err: err, Timestamp: time.Now()}:
	default:
	}
	close(turn.out)
}

func (p *PythonMistralRealtimeSTT) failWorker(err error) {
	p.mu.Lock()
	if p.workerErr != nil || p.closed {
		p.mu.Unlock()
		return
	}
	p.workerErr = err
	turn := p.current
	p.current = nil
	if p.stdin != nil {
		_ = p.stdin.Close()
		p.stdin = nil
	}
	p.mu.Unlock()

	if turn != nil {
		close(turn.out)
	}
}

func (p *PythonMistralRealtimeSTT) cancelCurrentLocked() {
	if p.current == nil {
		return
	}
	turn := p.current
	p.current = nil
	_ = p.writeCommandLocked(pythonMistralCmdCancel, nil)
	close(turn.out)
}

func (p *PythonMistralRealtimeSTT) writeCommandLocked(cmd byte, payload []byte) error {
	if p.stdin == nil {
		return fmt.Errorf("stt: Python Mistral worker is closed")
	}
	if _, err := p.stdin.Write([]byte{cmd}); err != nil {
		return fmt.Errorf("stt: write Python Mistral command: %w", err)
	}
	if payload == nil {
		return nil
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := p.stdin.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("stt: write Python Mistral payload length: %w", err)
	}
	if _, err := p.stdin.Write(payload); err != nil {
		return fmt.Errorf("stt: write Python Mistral payload: %w", err)
	}
	return nil
}

func (p *PythonMistralRealtimeSTT) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func pythonMistralWorkerPath() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("stt: resolve Python Mistral worker path")
	}
	return filepath.Join(filepath.Dir(file), "python_mistral_worker.py"), nil
}

func normalizeMistralServerURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "https://api.mistral.ai"
	}
	trimmed = strings.TrimRight(trimmed, "/")
	switch {
	case strings.HasPrefix(trimmed, "wss://"):
		return "https://" + strings.TrimPrefix(trimmed, "wss://")
	case strings.HasPrefix(trimmed, "ws://"):
		return "http://" + strings.TrimPrefix(trimmed, "ws://")
	case strings.HasPrefix(trimmed, "https://"), strings.HasPrefix(trimmed, "http://"):
		return trimmed
	default:
		return "https://" + trimmed
	}
}

func parsePythonMistralTimestamp(raw string) time.Time {
	if raw == "" {
		return time.Now()
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Now()
	}
	return parsed
}
