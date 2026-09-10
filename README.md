# Real-Time Voice Pipeline for Go

Reusable Go building blocks for real-time voice applications.

```text
audio → VAD → STT → transcript stabilization → LLM → phrase chunking → TTS → playback
```

The shared module owns the voice pipeline and provider interfaces. Each product
owns its prompts, domain logic, persistence, HTTP routes, and UI.

## Install

```powershell
go get github.com/etimbukafia/real-time-voice-pipeline-go@v0.1.0
```

Import only the packages your application needs:

```go
import (
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"github.com/etimbukafia/real-time-voice-pipeline-go/pipeline"
	"github.com/etimbukafia/real-time-voice-pipeline-go/vad"
)
```

## Packages

| Package | Provides |
| --- | --- |
| `audio` | PCM frame contracts, microphone capture, and WebSocket sources |
| `vad` | Voice activity detection, energy VAD, and optional Silero integration |
| `stt` | Streaming transcription contracts and provider clients |
| `llm` | Streaming completion contracts and the Mistral client |
| `tts` | Streaming speech synthesis contracts and the Cartesia client |
| `playback` | Speaker and WebSocket playback sinks |
| `pipeline` | Turn lifecycle, pre-roll, stabilization, cancellation, chunking, and metrics |
| `session` | Bounded-concurrency session orchestration |

The `cmd/voice-pipeline` program is a generic composition example. The packages
above are the reusable application boundary.

## Applications

- [Interview Coach](https://github.com/etimbukafia/interview-coach)
- [Speech Coach](https://github.com/etimbukafia/speech-coach)

## Development

```powershell
go test ./...
go vet ./...
go build ./...
```

Use `.env.example` as the configuration reference for the generic command:

```powershell
go run ./cmd/voice-pipeline
```

Optional build tags:

- `portaudio` enables native microphone and speaker support.
- `silero` enables the Silero VAD implementation.
