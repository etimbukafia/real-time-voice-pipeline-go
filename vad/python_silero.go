package vad

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
)

const (
	pythonSileroCmdFrame = 0x01
	pythonSileroCmdReset = 0x02
	pythonSileroCmdClose = 0x03

	pythonSileroRespFalse = 0x00
	pythonSileroRespTrue  = 0x01
	pythonSileroRespAck   = 0x02
)

type PythonSileroConfig struct {
	PythonExe  string
	ModelPath  string
	SampleRate int
	Threshold  float64
}

// PythonSileroVAD runs a persistent Python worker so Windows can use
// Silero without relying on the current CGO bridge.
type PythonSileroVAD struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	stderr *bytes.Buffer
	mu     sync.Mutex
}

func NewPythonSileroVAD(cfg PythonSileroConfig) (*PythonSileroVAD, error) {
	if cfg.PythonExe == "" {
		cfg.PythonExe = "python"
	}
	if cfg.ModelPath == "" {
		return nil, fmt.Errorf("vad: Python Silero model path is required")
	}
	if cfg.SampleRate == 0 {
		cfg.SampleRate = audio.SampleRate
	}
	if cfg.Threshold <= 0 || cfg.Threshold >= 1 {
		cfg.Threshold = 0.5
	}

	scriptPath, err := pythonSileroWorkerPath()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(
		cfg.PythonExe,
		"-u",
		scriptPath,
		"--model", cfg.ModelPath,
		"--sample-rate", fmt.Sprintf("%d", cfg.SampleRate),
		"--threshold", fmt.Sprintf("%.4f", cfg.Threshold),
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("vad: create Python stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("vad: create Python stdout pipe: %w", err)
	}
	stderrBuf := &bytes.Buffer{}
	cmd.Stderr = stderrBuf

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("vad: start Python Silero worker: %w", err)
	}

	v := &PythonSileroVAD{
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdoutPipe),
		stderr: stderrBuf,
	}

	ready := make([]byte, 4)
	if _, err := io.ReadFull(v.stdout, ready); err != nil {
		_ = v.Destroy()
		return nil, fmt.Errorf("vad: Python Silero worker failed to initialize: %w; stderr: %s", err, stderrBuf.String())
	}
	if string(ready) != "RDY1" {
		_ = v.Destroy()
		return nil, fmt.Errorf("vad: Python Silero worker sent unexpected handshake %q; stderr: %s", string(ready), stderrBuf.String())
	}

	return v, nil
}

func (v *PythonSileroVAD) Process(frame *audio.AudioFrame) (bool, error) {
	if frame == nil || len(frame.Data) == 0 {
		return false, nil
	}
	if len(frame.Data)%2 != 0 {
		return false, fmt.Errorf("vad: malformed PCM16 frame: expected even number of bytes, got %d", len(frame.Data))
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if err := v.writeCommand(pythonSileroCmdFrame, frame.Data); err != nil {
		return false, err
	}

	resp, err := v.stdout.ReadByte()
	if err != nil {
		return false, fmt.Errorf("vad: read Python Silero response: %w; stderr: %s", err, v.stderr.String())
	}
	switch resp {
	case pythonSileroRespFalse:
		return false, nil
	case pythonSileroRespTrue:
		return true, nil
	default:
		return false, fmt.Errorf("vad: unexpected Python Silero response byte 0x%02x", resp)
	}
}

func (v *PythonSileroVAD) Reset() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if err := v.writeCommand(pythonSileroCmdReset, nil); err != nil {
		return err
	}
	resp, err := v.stdout.ReadByte()
	if err != nil {
		return fmt.Errorf("vad: read Python Silero reset ack: %w; stderr: %s", err, v.stderr.String())
	}
	if resp != pythonSileroRespAck {
		return fmt.Errorf("vad: unexpected Python Silero reset ack 0x%02x", resp)
	}
	return nil
}

func (v *PythonSileroVAD) Destroy() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.stdin != nil {
		_ = v.writeCommand(pythonSileroCmdClose, nil)
		_ = v.stdin.Close()
		v.stdin = nil
	}
	if v.cmd == nil {
		return nil
	}

	err := v.cmd.Wait()
	v.cmd = nil
	if err != nil {
		return fmt.Errorf("vad: wait for Python Silero worker: %w; stderr: %s", err, v.stderr.String())
	}
	return nil
}

func (v *PythonSileroVAD) writeCommand(cmd byte, payload []byte) error {
	if v.stdin == nil {
		return fmt.Errorf("vad: Python Silero worker is closed")
	}

	header := []byte{cmd}
	if _, err := v.stdin.Write(header); err != nil {
		return fmt.Errorf("vad: write Python Silero command: %w", err)
	}

	if payload == nil {
		return nil
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := v.stdin.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("vad: write Python Silero payload length: %w", err)
	}
	if _, err := v.stdin.Write(payload); err != nil {
		return fmt.Errorf("vad: write Python Silero payload: %w", err)
	}
	return nil
}

func pythonSileroWorkerPath() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("vad: resolve Python Silero worker path")
	}
	return filepath.Join(filepath.Dir(file), "python_silero_worker.py"), nil
}
