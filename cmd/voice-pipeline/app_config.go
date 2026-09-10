package main

import (
	"context"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"github.com/etimbukafia/real-time-voice-pipeline-go/llm"
	"github.com/etimbukafia/real-time-voice-pipeline-go/pipeline"
	"github.com/etimbukafia/real-time-voice-pipeline-go/playback"
	"github.com/etimbukafia/real-time-voice-pipeline-go/stt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/tts"
	"github.com/etimbukafia/real-time-voice-pipeline-go/vad"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// appConfig holds the environment-driven wiring choices used by the program entrypoint.
type appConfig struct {
	sessionID               string
	systemPrompt            string
	audioSource             string
	audioSourceURL          string
	playbackSink            string
	playbackURL             string
	speechStartMS           int
	speechEndMS             int
	stableRepeats           int
	phraseMinChars          int
	maxConversationMessages int
	maxTurnAudioBuffer      int
	maxTokens               int
	temperature             float64

	vadProvider                   string
	sileroModelPath               string
	energyThreshold               float64
	pythonExe                     string
	vadPythonExe                  string
	sileroThreshold               float64
	sttProvider                   string
	sttPythonExe                  string
	mistralAPIKey                 string
	mistralBaseURL                string
	voxtralRealtimeURL            string
	voxtralTargetStreamingDelayMS int
	voxtralModel                  string
	llmModel                      string

	cartesiaAPIKey   string
	cartesiaVersion  string
	cartesiaVoiceID  string
	cartesiaLanguage string
	cartesiaModelID  string
}

// loadAppConfig is the composition-root configuration reader.
//
// In Python you might build a dict of settings and pass it around. This is the
// same idea, but expressed as a typed Go struct so the rest of the program gets
// field names with compile-time checking instead of ad-hoc string lookups.
func loadAppConfig() appConfig {
	return appConfig{
		sessionID:               envString("PIPELINE_SESSION_ID", "voice-session"),
		systemPrompt:            envString("PIPELINE_SYSTEM_PROMPT", "You are a concise voice assistant. Reply in one or two short sentences."),
		audioSource:             envString("PIPELINE_AUDIO_SOURCE", "mic"),
		audioSourceURL:          os.Getenv("PIPELINE_AUDIO_SOURCE_URL"),
		playbackSink:            envString("PIPELINE_PLAYBACK_SINK", "portaudio"),
		playbackURL:             os.Getenv("PIPELINE_PLAYBACK_URL"),
		speechStartMS:           envInt("PIPELINE_SPEECH_START_MS", 80),
		speechEndMS:             envInt("PIPELINE_SPEECH_END_MS", 400),
		stableRepeats:           envInt("PIPELINE_STABLE_REPEATS", 2),
		phraseMinChars:          envInt("PIPELINE_PHRASE_MIN_CHARS", 24),
		maxConversationMessages: envInt("PIPELINE_MAX_CONVERSATION_MESSAGES", 8),
		maxTurnAudioBuffer:      envInt("PIPELINE_MAX_TURN_AUDIO_BUFFER", 32),
		maxTokens:               envInt("PIPELINE_MAX_TOKENS", 120),
		temperature:             envFloat("PIPELINE_TEMPERATURE", 0.2),

		vadProvider:     envString("PIPELINE_VAD_PROVIDER", "silero"),
		sileroModelPath: strings.TrimSpace(os.Getenv("SILERO_MODEL_PATH")),
		energyThreshold: envFloat("PIPELINE_ENERGY_VAD_THRESHOLD", 0.04),
		pythonExe:       envString("PIPELINE_PYTHON_EXE", "python"),
		vadPythonExe:    strings.TrimSpace(os.Getenv("PIPELINE_VAD_PYTHON_EXE")),
		sileroThreshold: envFloat("PIPELINE_SILERO_THRESHOLD", 0.5),
		sttProvider:     envString("PIPELINE_STT_PROVIDER", "python-mistral"),
		sttPythonExe:    strings.TrimSpace(os.Getenv("PIPELINE_STT_PYTHON_EXE")),

		mistralAPIKey:                 strings.TrimSpace(os.Getenv("MISTRAL_API_KEY")),
		mistralBaseURL:                envString("MISTRAL_BASE_URL", "https://api.mistral.ai"),
		voxtralRealtimeURL:            strings.TrimSpace(os.Getenv("VOXTRAL_REALTIME_URL")),
		voxtralTargetStreamingDelayMS: envInt("VOXTRAL_TARGET_STREAMING_DELAY_MS", 0),
		voxtralModel:                  envString("VOXTRAL_MODEL", "voxtral-mini-transcribe-realtime-2602"),
		llmModel:                      envString("MISTRAL_LLM_MODEL", "mistral-small-latest"),

		cartesiaAPIKey:   strings.TrimSpace(os.Getenv("CARTESIA_API_KEY")),
		cartesiaVersion:  envString("CARTESIA_VERSION", "2025-04-16"),
		cartesiaVoiceID:  strings.TrimSpace(os.Getenv("CARTESIA_VOICE_ID")),
		cartesiaLanguage: envString("CARTESIA_LANGUAGE", "en"),
		cartesiaModelID:  envString("CARTESIA_MODEL_ID", "sonic-3"),
	}
}

// buildPipeline wires the real implementations behind the small interfaces used
// by the orchestrator.
//
// This is the main "assembly" function of the app. The pipeline package knows
// nothing about environment variables or provider-specific configuration. It
// only knows about interfaces. This file is where concrete providers are chosen
// and injected.
func buildPipeline(ctx context.Context, cfg appConfig, logger *log.Logger) (*pipeline.Pipeline, error) {
	source, err := buildAudioSource(ctx, cfg)
	if err != nil {
		return nil, err
	}

	vadBackend, err := buildVAD(cfg)
	if err != nil {
		return nil, err
	}

	sttClient, err := buildSTT(cfg)
	if err != nil {
		return nil, err
	}

	llmClient, err := buildLLM(cfg)
	if err != nil {
		return nil, err
	}

	ttsEngine, err := buildTTS(cfg)
	if err != nil {
		return nil, err
	}

	player, err := buildPlayer(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &pipeline.Pipeline{
		Source:  source,
		Tracker: vad.NewStateTracker(vadBackend, cfg.speechStartMS, cfg.speechEndMS),
		STT:     sttClient,
		LLM:     llmClient,
		TTS:     ttsEngine,
		Player:  player,
		Config: pipeline.Config{
			SessionID:               cfg.sessionID,
			SystemPrompt:            cfg.systemPrompt,
			LLMModel:                cfg.llmModel,
			TTSModel:                cfg.cartesiaModelID,
			VoiceID:                 cfg.cartesiaVoiceID,
			Language:                cfg.cartesiaLanguage,
			Temperature:             cfg.temperature,
			MaxTokens:               cfg.maxTokens,
			StableRepeats:           cfg.stableRepeats,
			PhraseMinChars:          cfg.phraseMinChars,
			MaxConversationMessages: cfg.maxConversationMessages,
			MaxTurnAudioBuffer:      cfg.maxTurnAudioBuffer,
			Logger:                  logger,
		},
	}, nil
}

// buildAudioSource chooses where incoming PCM frames come from.
//
// The rest of the system does not care whether audio came from a microphone or
// a websocket. That is exactly why `audio.AudioSource` exists as an interface:
// keep acquisition concerns outside the realtime orchestration logic.
func buildAudioSource(ctx context.Context, cfg appConfig) (audio.AudioSource, error) {
	switch strings.ToLower(cfg.audioSource) {
	case "mic":
		return audio.NewMicSource()
	case "websocket":
		if strings.TrimSpace(cfg.audioSourceURL) == "" {
			return nil, fmt.Errorf("PIPELINE_AUDIO_SOURCE_URL is required when PIPELINE_AUDIO_SOURCE=websocket")
		}
		return audio.DialWebSocketSource(ctx, cfg.audioSourceURL, nil)
	default:
		return nil, fmt.Errorf("unknown PIPELINE_AUDIO_SOURCE %q; expected `mic` or `websocket`", cfg.audioSource)
	}
}

// buildVAD chooses the voice activity detector implementation.
//
// `silero` is the real provider path. `energy` exists as a very small fallback
// implementation that is useful for constrained environments, but the pipeline
// still consumes both through the same `vad.VAD` contract.
func buildVAD(cfg appConfig) (vad.VAD, error) {
	switch strings.ToLower(cfg.vadProvider) {
	case "silero":
		if strings.TrimSpace(cfg.sileroModelPath) == "" {
			return nil, fmt.Errorf("SILERO_MODEL_PATH is required when PIPELINE_VAD_PROVIDER=silero")
		}
		pythonExe := cfg.vadPythonExe
		if pythonExe == "" {
			pythonExe = cfg.pythonExe
		}
		return vad.NewPythonSileroVAD(vad.PythonSileroConfig{
			PythonExe:  pythonExe,
			ModelPath:  cfg.sileroModelPath,
			SampleRate: audio.SampleRate,
			Threshold:  cfg.sileroThreshold,
		})
	case "energy":
		return vad.NewEnergyVAD(cfg.energyThreshold)
	default:
		return nil, fmt.Errorf("unknown PIPELINE_VAD_PROVIDER %q; expected `silero` or `energy`", cfg.vadProvider)
	}
}

// buildSTT wires the realtime Voxtral client.
//
// The interface boundary matters here: `pipeline.Run` should not know anything
// about auth headers or websocket URLs. Its only job is to feed audio into an
// `stt.STT` implementation and react to transcripts coming back.
func buildSTT(cfg appConfig) (stt.STT, error) {
	if strings.TrimSpace(cfg.mistralAPIKey) == "" {
		return nil, fmt.Errorf("MISTRAL_API_KEY is required for live STT")
	}
	switch strings.ToLower(cfg.sttProvider) {
	case "", "python-mistral":
		pythonExe := cfg.sttPythonExe
		if pythonExe == "" {
			pythonExe = cfg.pythonExe
		}
		return stt.NewPythonMistralRealtimeSTT(stt.PythonMistralRealtimeConfig{
			PythonExe:              pythonExe,
			APIKey:                 cfg.mistralAPIKey,
			Model:                  cfg.voxtralModel,
			ServerURL:              normalizeMistralBaseURL(cfg.mistralBaseURL, cfg.voxtralRealtimeURL),
			TargetStreamingDelayMS: cfg.voxtralTargetStreamingDelayMS,
		})
	case "go-websocket":
		if strings.TrimSpace(cfg.voxtralRealtimeURL) == "" {
			return nil, fmt.Errorf("VOXTRAL_REALTIME_URL is required when PIPELINE_STT_PROVIDER=go-websocket")
		}
		header := http.Header{}
		header.Set("Authorization", "Bearer "+cfg.mistralAPIKey)
		return stt.NewMistralVoxtralSTT(cfg.voxtralModel, cfg.voxtralRealtimeURL, header)
	default:
		return nil, fmt.Errorf("unknown PIPELINE_STT_PROVIDER %q; expected `python-mistral` or `go-websocket`", cfg.sttProvider)
	}
}

// buildLLM wires the Mistral chat client behind the `llm.Client` interface.
func buildLLM(cfg appConfig) (llm.Client, error) {
	if strings.TrimSpace(cfg.mistralAPIKey) == "" {
		return nil, fmt.Errorf("MISTRAL_API_KEY is required for live LLM")
	}
	return llm.NewMistralClient(cfg.mistralAPIKey, cfg.llmModel)
}

// buildTTS wires the Cartesia client behind the `tts.Engine` interface.
func buildTTS(cfg appConfig) (tts.Engine, error) {
	if strings.TrimSpace(cfg.cartesiaAPIKey) == "" {
		return nil, fmt.Errorf("CARTESIA_API_KEY is required for live TTS")
	}
	if strings.TrimSpace(cfg.cartesiaVoiceID) == "" {
		return nil, fmt.Errorf("CARTESIA_VOICE_ID is required for live TTS")
	}

	engine, err := tts.NewCartesiaEngine(cfg.cartesiaAPIKey)
	if err != nil {
		return nil, err
	}
	engine.Version = cfg.cartesiaVersion
	engine.ModelID = cfg.cartesiaModelID
	engine.VoiceID = cfg.cartesiaVoiceID
	engine.Language = cfg.cartesiaLanguage
	return engine, nil
}

// buildPlayer chooses where assistant audio goes.
//
// Like the input side, playback is kept behind an interface so the pipeline can
// focus on timing and cancellation rather than transport details.
func buildPlayer(ctx context.Context, cfg appConfig) (playback.Player, error) {
	switch strings.ToLower(cfg.playbackSink) {
	case "portaudio":
		return playback.NewPortAudioPlayer()
	case "websocket":
		if strings.TrimSpace(cfg.playbackURL) == "" {
			return nil, fmt.Errorf("PIPELINE_PLAYBACK_URL is required when PIPELINE_PLAYBACK_SINK=websocket")
		}
		return playback.DialWebSocketPlayer(ctx, cfg.playbackURL, nil)
	default:
		return nil, fmt.Errorf("unknown PIPELINE_PLAYBACK_SINK %q; expected `portaudio` or `websocket`", cfg.playbackSink)
	}
}

// envString reads a string environment variable and falls back when it is unset.
func envString(key, fallback string) string {
	// The env helpers intentionally fail soft by returning defaults on missing or
	// malformed values. That keeps configuration parsing small and predictable,
	// while the provider builders above still enforce the truly required values.
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

// envInt reads an integer environment variable and falls back on empty or invalid input.
func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

// envFloat reads a float environment variable and falls back on empty or invalid input.
func envFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func normalizeMistralBaseURL(baseURL, legacyRealtimeURL string) string {
	if trimmed := strings.TrimSpace(baseURL); trimmed != "" {
		return rewriteMistralScheme(trimmed)
	}
	if trimmed := strings.TrimSpace(legacyRealtimeURL); trimmed != "" {
		return rewriteMistralScheme(trimmed)
	}
	return "https://api.mistral.ai"
}

func rewriteMistralScheme(raw string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
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
