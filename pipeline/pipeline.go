package pipeline

import (
	"context"
	"errors"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"github.com/etimbukafia/real-time-voice-pipeline-go/llm"
	"github.com/etimbukafia/real-time-voice-pipeline-go/playback"
	"github.com/etimbukafia/real-time-voice-pipeline-go/stt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/tts"
	"github.com/etimbukafia/real-time-voice-pipeline-go/vad"
	"log"
	"strings"
	"time"
)

// Pipeline wires the realtime voice stages together for one live session.
type Pipeline struct {
	Source     audio.AudioSource
	Tracker    *vad.StateTracker
	STT        stt.STT
	Stabilizer *TranscriptStabilizer
	LLM        llm.Client
	TTS        tts.Engine
	Player     playback.Player
	Config     Config
}

// liveTurn holds the cancelable per-turn state while one user utterance is in flight.
type liveTurn struct {
	id            string
	index         int
	ctx           context.Context
	cancel        context.CancelFunc
	audioIn       chan *audio.AudioFrame
	captureClosed bool
}

// transcriptResult is the STT outcome sent back from a turn-local worker to the session loop.
type transcriptResult struct {
	turnID     string
	finalText  string
	receivedAt time.Time
	err        error
}

// responseResult is the assistant-side result produced after LLM, TTS, and playback finish.
type responseResult struct {
	turnID            string
	assistantText     string
	llmStartedAt      time.Time
	llmFirstTokenAt   time.Time
	ttsFirstAudioAt   time.Time
	playbackStartedAt time.Time
	finishedAt        time.Time
	err               error
}

// responseProgress reports early assistant milestones such as first synthesized audio.
type responseProgress struct {
	turnID       string
	firstAudioAt time.Time
}

// Run is the session-level control loop.
//
// If you are coming from Python, the easiest way to read this function is:
//
//  1. one goroutine (this one) owns the session state machine
//  2. helper goroutines do background work for one turn at a time
//  3. helper goroutines report back through channels
//  4. this loop decides what state the session is in and what happens next
//
// That ownership rule is important. It keeps the mutable session state
// (`current`, `conversation`, `state`, metrics) in one place instead of letting
// several goroutines mutate it concurrently.
func (p *Pipeline) Run(ctx context.Context) (*Summary, error) {
	cfg := p.configWithDefaults()
	logger := cfg.Logger

	if p.Source == nil {
		return nil, fmt.Errorf("pipeline: source is required")
	}
	if p.Tracker == nil {
		return nil, fmt.Errorf("pipeline: tracker is required")
	}
	if p.STT == nil {
		return nil, fmt.Errorf("pipeline: STT client is required")
	}
	if p.LLM == nil {
		return nil, fmt.Errorf("pipeline: LLM client is required")
	}
	if p.TTS == nil {
		return nil, fmt.Errorf("pipeline: TTS engine is required")
	}
	if p.Player == nil {
		return nil, fmt.Errorf("pipeline: player is required")
	}
	if err := warmIfSupported(ctx, "STT", p.STT); err != nil {
		return nil, err
	}
	if err := warmIfSupported(ctx, "TTS", p.TTS); err != nil {
		return nil, err
	}
	defer closeIfSupported(logger, "STT", p.STT)
	defer closeIfSupported(logger, "LLM", p.LLM)
	defer closeIfSupported(logger, "TTS", p.TTS)

	frames, err := p.Source.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("pipeline: start source: %w", err)
	}

	stabilizer := p.Stabilizer
	if stabilizer == nil {
		stabilizer = NewTranscriptStabilizer(cfg.StableRepeats)
	}

	summary := &Summary{SessionID: cfg.SessionID}
	turnIndex := make(map[string]int)
	preRoll := newPreRollBuffer(cfg.PreRollFrames)
	transcriptResults := make(chan transcriptResult, 8)
	responseResults := make(chan responseResult, 8)
	responseProgresses := make(chan responseProgress, 8)

	state := StateListening
	setState := func(next SessionState) {
		if state == next {
			return
		}
		logger.Printf("session=%s state=%s", cfg.SessionID, next)
		state = next
	}

	var current *liveTurn
	var conversation []llm.Message
	turnSeq := 0

	for {
		// We are done only when the input source has ended AND there is no active
		// turn still being processed. That second condition matters because STT,
		// LLM, and TTS may still be finishing work after the input source closes.
		if frames == nil && current == nil {
			return summary, nil
		}

		select {
		case <-ctx.Done():
			// The session context is the top-level cancellation signal.
			// Cancel the active turn so its helper goroutines shut down too.
			if current != nil {
				current.cancel()
			}
			return summary, ctx.Err()

		case result := <-transcriptResults:
			// Transcript results come back from the turn-local STT pipeline.
			// Only the main loop is allowed to decide whether that result is still
			// relevant. If the turn was superseded, we simply ignore it here.
			idx, ok := turnIndex[result.turnID]
			if !ok {
				continue
			}
			metrics := &summary.Turns[idx]
			if result.err != nil {
				if !errors.Is(result.err, context.Canceled) {
					metrics.Error = result.err.Error()
				}
				if metrics.FinishedAt.IsZero() {
					metrics.FinishedAt = time.Now()
				}
				if current != nil && current.id == result.turnID {
					current = nil
					setState(StateListening)
				}
				continue
			}

			metrics.UserText = result.finalText
			metrics.TranscriptFinalAt = result.receivedAt
			logger.Printf("turn=%s transcript_final=%q stt_latency=%s", result.turnID, result.finalText, metrics.EndOfSpeechToSTTFinal())

			if current == nil || current.id != result.turnID {
				continue
			}

			setState(StateThinking)
			requestMessages := appendConversation(conversation, cfg.MaxConversationMessages)
			requestMessages = append(requestMessages, llm.Message{
				Role:    "user",
				Content: result.finalText,
			})
			turnCtx := current.ctx

			go func(turnCtx context.Context, turnID string, request llm.Request) {
				// The assistant response path is expensive and fully cancelable.
				// We run it in a helper goroutine so the main loop can keep reacting
				// to new audio immediately, including user barge-in.
				//
				// The guarded send below is subtle but important:
				// if the turn is canceled, do not block forever trying to report a
				// result back into a session loop that may no longer care.
				result := p.runAssistantResponse(turnCtx, turnID, request, cfg, responseProgresses)
				select {
				case <-turnCtx.Done():
				case responseResults <- result:
				}
			}(turnCtx, current.id, llm.Request{
				Model:        cfg.LLMModel,
				SystemPrompt: cfg.SystemPrompt,
				Messages:     requestMessages,
				MaxTokens:    cfg.MaxTokens,
				Temperature:  cfg.Temperature,
			})

		case progress := <-responseProgresses:
			idx, ok := turnIndex[progress.turnID]
			if !ok {
				continue
			}
			metrics := &summary.Turns[idx]
			if metrics.TTSFirstAudioAt.IsZero() {
				metrics.TTSFirstAudioAt = progress.firstAudioAt
			}
			setState(StateSpeaking)

		case result := <-responseResults:
			// A completed assistant turn comes back here after LLM + TTS + playback.
			idx, ok := turnIndex[result.turnID]
			if !ok {
				continue
			}
			metrics := &summary.Turns[idx]
			metrics.LLMStartedAt = result.llmStartedAt
			metrics.LLMFirstTokenAt = result.llmFirstTokenAt
			metrics.TTSFirstAudioAt = result.ttsFirstAudioAt
			metrics.PlaybackStartedAt = result.playbackStartedAt
			metrics.FinishedAt = result.finishedAt
			if result.assistantText != "" {
				metrics.AssistantText = strings.TrimSpace(result.assistantText)
			}
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				metrics.Error = result.err.Error()
			}

			if metrics.AssistantText != "" && metrics.UserText != "" && !metrics.Interrupted {
				conversation = append(conversation,
					llm.Message{Role: "user", Content: metrics.UserText},
					llm.Message{Role: "assistant", Content: metrics.AssistantText},
				)
				conversation = appendConversation(conversation, cfg.MaxConversationMessages)
			}

			logger.Printf(
				"turn=%s done interrupted=%t first_token=%s first_audio=%s playback=%s assistant=%q",
				result.turnID,
				metrics.Interrupted,
				metrics.EndOfSpeechToFirstToken(),
				metrics.EndOfSpeechToFirstAudio(),
				metrics.EndOfSpeechToPlayback(),
				metrics.AssistantText,
			)

			if current != nil && current.id == result.turnID {
				current.cancel()
				current = nil
				setState(StateListening)
			}

		case frame, ok := <-frames:
			if !ok {
				frames = nil
				if current != nil && !current.captureClosed {
					// Closing the turn audio channel is the signal to STT that the
					// utterance is complete and it should flush/finalize.
					close(current.audioIn)
					current.captureClosed = true
				}
				continue
			}
			if frame == nil {
				continue
			}

			event, _, err := p.Tracker.Process(frame)
			if err != nil {
				logger.Printf("pipeline: tracker error: %v", err)
				preRoll.Add(frame)
				continue
			}

			switch event {
			case vad.SpeechStart:
				if current != nil {
					// This is barge-in.
					//
					// The user started speaking while a previous turn was still
					// active, so the old turn is now stale. We mark it interrupted,
					// cancel its context, and immediately create a fresh turn.
					metrics := &summary.Turns[current.index]
					metrics.Interrupted = true
					metrics.InterruptedAt = frame.Timestamp
					logger.Printf("turn=%s interrupted by new speech", current.id)
					current.cancel()
					setState(StateInterrupted)
				}

				turnSeq++
				turnID := fmt.Sprintf("%s-turn-%03d", cfg.SessionID, turnSeq)
				index := summary.appendTurn(turnID, frame.Timestamp)
				turnIndex[turnID] = index
				current = p.startTurn(ctx, turnID, index, cfg, stabilizer, transcriptResults)

				setState(StateUserSpeaking)
				logger.Printf("turn=%s speech_start", turnID)

				// Feed the pre-roll frames first so STT does not miss the beginning
				// of the utterance. This is the compensation for waiting a short
				// time before trusting the VAD speech-start decision.
				for _, prior := range preRoll.Snapshot() {
					sendFrameLatest(logger, current.audioIn, prior)
				}
				sendFrameLatest(logger, current.audioIn, frame)

			default:
				if current != nil && !current.captureClosed {
					sendFrameLatest(logger, current.audioIn, frame)
				}
			}

			if event == vad.SpeechEnd && current != nil && !current.captureClosed {
				summary.Turns[current.index].UserSpeechEndedAt = frame.Timestamp
				logger.Printf("turn=%s speech_end", current.id)
				close(current.audioIn)
				current.captureClosed = true
				setState(StateThinking)
			}

			preRoll.Add(frame)
		}
	}
}

// startTurn creates the per-turn STT path.
//
// A "turn" is the smallest unit of work we can cancel safely. Each turn gets:
//   - its own context
//   - its own audio input channel
//   - its own STT stream
//   - its own transcript stabilizer output
//
// Keeping that boundary explicit makes barge-in practical: cancel one turn,
// start a new one, and ignore any late results from the old turn.
func (p *Pipeline) startTurn(
	parent context.Context,
	turnID string,
	index int,
	cfg Config,
	stabilizer *TranscriptStabilizer,
	results chan<- transcriptResult,
) *liveTurn {
	turnCtx, cancel := context.WithCancel(parent)
	audioIn := make(chan *audio.AudioFrame, cfg.MaxTurnAudioBuffer)

	transcripts, err := p.STT.Transcribe(turnCtx, audioIn)
	if err != nil {
		select {
		case <-turnCtx.Done():
		case results <- transcriptResult{
			turnID:     turnID,
			receivedAt: time.Now(),
			err:        err,
		}:
		}
		return &liveTurn{
			id:      turnID,
			index:   index,
			ctx:     turnCtx,
			cancel:  cancel,
			audioIn: audioIn,
		}
	}

	commits := stabilizer.Run(turnCtx, transcripts)
	logger := cfg.Logger

	go func() {
		// This goroutine is the bridge from raw STT events to the session loop.
		// It does not make policy decisions beyond "forward the final transcript".
		// The main loop still decides whether the result is current or stale.
		for commit := range commits {
			logger.Printf("turn=%s transcript_commit final=%t delta=%q text=%q", turnID, commit.Final, commit.Delta, commit.Text)
			if commit.Err != nil {
				select {
				case <-turnCtx.Done():
				case results <- transcriptResult{
					turnID:     turnID,
					receivedAt: commit.Timestamp,
					err:        commit.Err,
				}:
				}
				return
			}
			if !commit.Final {
				continue
			}

			select {
			case <-turnCtx.Done():
			case results <- transcriptResult{
				turnID:     turnID,
				finalText:  commit.Text,
				receivedAt: commit.Timestamp,
			}:
			}
			return
		}

		if turnCtx.Err() != nil {
			select {
			case <-turnCtx.Done():
			case results <- transcriptResult{
				turnID:     turnID,
				receivedAt: time.Now(),
				err:        turnCtx.Err(),
			}:
			}
		}
	}()

	return &liveTurn{
		id:      turnID,
		index:   index,
		ctx:     turnCtx,
		cancel:  cancel,
		audioIn: audioIn,
	}
}

// runAssistantResponse owns the assistant-side streaming path for one turn:
//
//	LLM stream -> phrase chunker -> TTS stream -> playback
//
// This is split out from Run so the main session loop can remain responsive to
// new user speech while the assistant is still generating or speaking.
func (p *Pipeline) runAssistantResponse(
	ctx context.Context,
	turnID string,
	req llm.Request,
	cfg Config,
	progress chan<- responseProgress,
) responseResult {
	result := responseResult{
		turnID:       turnID,
		llmStartedAt: time.Now(),
	}

	llmStream, err := p.LLM.Generate(ctx, req)
	if err != nil {
		result.err = fmt.Errorf("pipeline: LLM generate: %w", err)
		result.finishedAt = time.Now()
		return result
	}

	textToTTS := make(chan string, 8)
	ttsStream, err := p.TTS.Synthesize(ctx, tts.Request{
		ModelID:   cfg.TTSModel,
		VoiceID:   cfg.VoiceID,
		Language:  cfg.Language,
		ContextID: turnID,
	}, textToTTS)
	if err != nil {
		result.err = fmt.Errorf("pipeline: TTS synthesize: %w", err)
		result.finishedAt = time.Now()
		return result
	}

	playerInput := make(chan audio.AudioFrame, 16)
	playerDone := make(chan error, 1)
	go func() {
		// Playback is isolated in its own goroutine because it can block on audio
		// output timing. We keep it off the main assistant path so TTS forwarding
		// stays simple and cancellation can stop everything through the shared ctx.
		playerDone <- p.Player.Play(ctx, playerInput)
	}()

	llmDone := make(chan llmPumpResult, 1)
	go func() {
		// This helper turns token/delta streaming into phrase-sized TTS input.
		// We do this concurrently with reading TTS output so the pipeline overlaps
		// work instead of behaving like a strict sequence of blocking calls.
		llmDone <- pumpLLMToPhrases(ctx, llmStream, textToTTS, cfg.PhraseMinChars)
	}()

	for {
		select {
		case <-ctx.Done():
			close(playerInput)
			<-playerDone
			result.err = ctx.Err()
			result.finishedAt = time.Now()
			return result

		case chunk, ok := <-ttsStream:
			if !ok {
				close(playerInput)
				playerErr := <-playerDone
				pump := <-llmDone
				result.assistantText = pump.fullText
				result.llmFirstTokenAt = pump.firstTokenAt
				result.finishedAt = time.Now()
				if pump.err != nil && !errors.Is(pump.err, context.Canceled) {
					result.err = pump.err
					return result
				}
				if playerErr != nil && !errors.Is(playerErr, context.Canceled) {
					result.err = playerErr
				}
				return result
			}

			if chunk.Done {
				continue
			}

			if result.ttsFirstAudioAt.IsZero() {
				result.ttsFirstAudioAt = nonZeroTime(chunk.Timestamp)
				select {
				case progress <- responseProgress{turnID: turnID, firstAudioAt: result.ttsFirstAudioAt}:
				default:
				}
			}

			if result.playbackStartedAt.IsZero() {
				result.playbackStartedAt = time.Now()
			}

			select {
			case <-ctx.Done():
				close(playerInput)
				<-playerDone
				result.err = ctx.Err()
				result.finishedAt = time.Now()
				return result
			case playerInput <- chunk.Frame:
			}
		}
	}
}

// llmPumpResult captures the accumulated assistant text plus timing and error state from LLM pumping.
type llmPumpResult struct {
	fullText     string
	firstTokenAt time.Time
	err          error
}

// pumpLLMToPhrases converts token-level LLM chunks into phrase-level TTS requests.
func pumpLLMToPhrases(ctx context.Context, stream <-chan llm.Chunk, out chan<- string, minChars int) llmPumpResult {
	defer close(out)

	chunker := NewPhraseChunker(minChars)
	var builder strings.Builder
	var firstTokenAt time.Time

	for {
		select {
		case <-ctx.Done():
			return llmPumpResult{err: ctx.Err()}
		case chunk, ok := <-stream:
			if !ok {
				if tail := chunker.Flush(); tail != "" {
					if !sendPhrase(ctx, out, tail) {
						return llmPumpResult{err: ctx.Err()}
					}
				}
				return llmPumpResult{
					fullText:     strings.TrimSpace(builder.String()),
					firstTokenAt: firstTokenAt,
				}
			}

			if chunk.Text != "" {
				if firstTokenAt.IsZero() {
					firstTokenAt = nonZeroTime(chunk.Timestamp)
				}

				// We keep two different views of the LLM output:
				//   - `builder`: the full assistant text for metrics/history
				//   - `chunker`: the phrase buffer for TTS
				//
				// Those are related but not identical responsibilities, so it is
				// clearer to store them separately.
				builder.WriteString(chunk.Text)
				for _, phrase := range chunker.Push(chunk.Text) {
					if !sendPhrase(ctx, out, phrase) {
						return llmPumpResult{err: ctx.Err()}
					}
				}
			}

			if chunk.Final {
				if tail := chunker.Flush(); tail != "" {
					if !sendPhrase(ctx, out, tail) {
						return llmPumpResult{err: ctx.Err()}
					}
				}
				return llmPumpResult{
					fullText:     strings.TrimSpace(builder.String()),
					firstTokenAt: firstTokenAt,
				}
			}
		}
	}
}

// sendPhrase forwards non-empty text to TTS unless the turn has already been canceled.
func sendPhrase(ctx context.Context, out chan<- string, phrase string) bool {
	if strings.TrimSpace(phrase) == "" {
		return true
	}

	select {
	case <-ctx.Done():
		return false
	case out <- phrase:
		return true
	}
}

// sendFrameLatest keeps the freshest audio in a bounded buffer by evicting stale frames when needed.
func sendFrameLatest(logger *log.Logger, out chan *audio.AudioFrame, frame *audio.AudioFrame) {
	if frame == nil {
		return
	}

	select {
	case out <- frame:
	default:
		// This buffer is intentionally freshness-biased.
		// If the turn-local consumer falls behind, we would rather drop stale
		// buffered audio than let the whole session drift behind real time.
		select {
		case <-out:
		default:
		}

		select {
		case out <- frame:
		default:
			logger.Printf("pipeline: dropped frame because turn buffer stayed full")
		}
	}
}

// appendConversation returns a bounded copy of the latest conversation history.
func appendConversation(messages []llm.Message, max int) []llm.Message {
	if max <= 0 || len(messages) <= max {
		out := make([]llm.Message, len(messages))
		copy(out, messages)
		return out
	}

	start := len(messages) - max
	out := make([]llm.Message, max)
	copy(out, messages[start:])
	return out
}

// configWithDefaults fills in safe runtime defaults so the session loop has a complete config.
func (p *Pipeline) configWithDefaults() Config {
	cfg := p.Config
	if cfg.SessionID == "" {
		cfg.SessionID = fmt.Sprintf("session-%d", time.Now().UnixNano())
	}
	if cfg.LLMModel == "" {
		cfg.LLMModel = "mistral-small-latest"
	}
	if cfg.TTSModel == "" {
		cfg.TTSModel = "sonic-3"
	}
	if cfg.Language == "" {
		cfg.Language = "en"
	}
	if cfg.PreRollFrames <= 0 {
		cfg.PreRollFrames = 5
	}
	if cfg.StableRepeats <= 0 {
		cfg.StableRepeats = 2
	}
	if cfg.PhraseMinChars <= 0 {
		cfg.PhraseMinChars = 24
	}
	if cfg.MaxConversationMessages <= 0 {
		cfg.MaxConversationMessages = 8
	}
	if cfg.MaxTurnAudioBuffer <= 0 {
		cfg.MaxTurnAudioBuffer = 32
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = 120
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return cfg
}

// nonZeroTime replaces a missing provider timestamp with the local current time.
func nonZeroTime(ts time.Time) time.Time {
	if ts.IsZero() {
		return time.Now()
	}
	return ts
}

// warmable is implemented by providers that can pre-open session resources.
type warmable interface {
	Warm(ctx context.Context) error
}

// closeable is implemented by providers that own session resources.
type closeable interface {
	Close() error
}

// warmIfSupported pays connection setup at session start instead of during the first turn.
func warmIfSupported(ctx context.Context, name string, provider any) error {
	warm, ok := provider.(warmable)
	if !ok {
		return nil
	}
	if err := warm.Warm(ctx); err != nil {
		return fmt.Errorf("pipeline: warm %s provider: %w", name, err)
	}
	return nil
}

// closeIfSupported releases provider resources without forcing every fake to implement Close.
func closeIfSupported(logger *log.Logger, name string, provider any) {
	closer, ok := provider.(closeable)
	if !ok {
		return
	}
	if err := closer.Close(); err != nil {
		logger.Printf("pipeline: close %s provider: %v", name, err)
	}
}
