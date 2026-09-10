# Real-Time Voice Pipeline for Go

An importable, reusable foundation for real-time voice products:

`audio -> VAD -> STT -> transcript stabilization -> LLM -> phrase chunking -> TTS -> playback`

Product-specific applications live in their own repositories and build on this
module instead of copying the voice stack:

- [interview-coach](https://github.com/etimbukafia/interview-coach)
- [speech-coach](https://github.com/etimbukafia/speech-coach)

## Packages

- `audio`: PCM frame contracts, microphone capture, and websocket sources
- `vad`: VAD contracts, energy VAD, optional Silero integration, and speech state tracking
- `stt`: streaming transcription contracts and provider clients
- `llm`: streaming completion contracts and Mistral client
- `tts`: streaming speech synthesis contracts and Cartesia client
- `playback`: playback contracts, PortAudio playback, and websocket playback
- `pipeline`: pre-roll, turn lifecycle, transcript stabilization, cancellation, chunking, and metrics
- `session`: bounded-concurrency session orchestration

The optional `cmd/voice-pipeline` command is a small generic composition example;
the packages above are the reusable product boundary.

## Use from another Go module

```powershell
go get github.com/etimbukafia/real-time-voice-pipeline-go@v0.1.0
```

Then import the focused packages you need:

```go
import (
    "github.com/etimbukafia/real-time-voice-pipeline-go/audio"
    "github.com/etimbukafia/real-time-voice-pipeline-go/pipeline"
    "github.com/etimbukafia/real-time-voice-pipeline-go/vad"
)
```

Applications own their prompts, domain models, persistence, HTTP routes, and
composition-root configuration. The shared pipeline only depends on interfaces
for provider and transport implementations.

## Development

```powershell
go test ./...
go vet ./...
go build ./...
```

The generic example can be run after setting the values in `.env.example`:

```powershell
go run ./cmd/voice-pipeline
```

Build tags:

- `portaudio`: enable native microphone and speaker support
- `silero`: enable the Silero VAD implementation

For browser-based applications, use websocket audio sources and playback sinks;
the browser transport and UI remain owned by each standalone app.

## Documentation

- [VOICE_PIPELINE_GUIDE.md](VOICE_PIPELINE_GUIDE.md): architecture and reading order
- [GO_CODEBASE_TEACHING_GUIDE.md](GO_CODEBASE_TEACHING_GUIDE.md): Go design walkthrough
- [GO_CONCURRENCY_THROUGH_PIPELINE.md](GO_CONCURRENCY_THROUGH_PIPELINE.md): concurrency decisions
