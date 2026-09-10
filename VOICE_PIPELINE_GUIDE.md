# Voice Pipeline Guide

This is the document to study alongside the code.

It explains the architecture and design decisions of the reusable code that
exists in this repository.

## 1. What this codebase is trying to optimize

The pipeline is optimized for one thing first: responsiveness.

That means several design choices dominate the code:

- We prefer bounded queues over unbounded buffering.
- We prefer canceling stale work over finishing work that no longer matters.
- We prefer overlapping stages over waiting for one stage to fully complete before starting the next.
- We prefer freshness over replaying a backlog in realtime paths.
- We keep the interfaces small so each stage can be reasoned about independently.

That is the core mindset behind a good voice system.

The mistake many first implementations make is treating the whole turn as one blocking function:

1. record audio
2. run STT
3. run LLM
4. run TTS
5. play audio

That architecture is easy to write but structurally slow. This repository does not do that.

## 2. The actual runtime shape

At runtime, one session looks like this:

1. `audio.AudioSource` produces PCM16 frames.
2. `vad.StateTracker` consumes those frames and emits stable speech boundaries.
3. When speech starts, the pipeline opens a turn-local STT stream.
4. When speech ends, the turn-local STT stream is closed so the recognizer can flush its final transcript.
5. `pipeline.TranscriptStabilizer` converts noisy cumulative partials into trustworthy commits.
6. The final transcript is appended to conversation history and sent to the LLM.
7. LLM deltas are grouped by `pipeline.PhraseChunker` into TTS-friendly phrases.
8. TTS returns audio chunks as they become available.
9. Playback starts as soon as the first audio chunk arrives.
10. If the user starts speaking while the assistant is still thinking or speaking, the pipeline cancels that turn immediately.

The important thing here is that the system is not "function composition"; it is turn orchestration under cancellation.

## 3. The state machine

The session state machine is intentionally small:

- `listening`
- `user_speaking`
- `thinking`
- `speaking`
- `interrupted`

Why keep it this small?

Because voice systems become fragile when their state space becomes vague. If you cannot say clearly what state a call is in, you will eventually:

- play stale audio
- send new audio into the wrong STT stream
- race old and new assistant turns
- fail to stop speaking when the user barges in

In this codebase, `pipeline.Run` is the control loop that owns those transitions.

## 4. Why the pre-roll buffer exists

Study `pipeline/preroll.go`.

This is one of the most important low-level details in the project.

Raw VAD is noisy, so we do not trust the very first frame that looks like speech. We require multiple consecutive speech frames before confirming `SpeechStart`. That improves correctness, but it creates a new problem: by the time we confirm speech, the user has already been talking for roughly 50-100ms.

If we started STT only at the confirmation moment, the beginning of the utterance would be clipped.

The pre-roll buffer solves that:

- keep the last few frames before confirmation
- when speech start is confirmed, inject them into the new turn
- then continue with live frames

That is a classic realtime tradeoff:

- smoothing gives stability
- pre-roll gives back the latency we spent to get that stability

## 5. Why transcript stabilization exists

Study `pipeline/stabilizer.go`.

Streaming STT is noisy by nature. A recognizer often revises the text as more audio arrives. If you feed every partial transcript directly into the LLM, you create three problems:

- unnecessary LLM churn
- unstable planning
- wasted latency and cost

The stabilizer exists to answer one question:

"Which part of the transcript is stable enough that I am willing to build the rest of the system on top of it?"

The implementation here uses a practical heuristic:

- compare consecutive cumulative partials
- find the shared stable prefix
- only commit text when that prefix has repeated enough times
- always trust the final transcript

This is intentionally simpler than a research-grade alignment algorithm, but it has the right architecture. You can evolve the heuristic later without changing the rest of the pipeline contract.

## 6. Why the LLM does not feed TTS token-by-token

Study `pipeline/chunker.go`.

A naive streaming implementation would send every LLM token directly to TTS. That feels "maximally streaming," but it is usually the wrong shape for speech.

Why?

- TTS on tiny fragments sounds unnatural.
- Too many tiny synthesis requests increase overhead.
- Prosody needs phrase-level structure.

The chunker is the compromise:

- flush at punctuation when possible
- flush at whitespace when the buffer gets too large
- keep the buffer small enough that playback can start early

This is a latency-versus-naturalness tradeoff. The code makes that tradeoff explicit.

## 7. Why each turn gets a logical STT stream

The first version opened a new STT websocket for each user utterance because
that is the simplest lifecycle to reason about.

The current version is optimized for lower latency:

- the Voxtral websocket is warmed at session start
- normal turns reuse that websocket instead of reconnecting
- each user utterance still gets its own logical transcript channel
- VAD endpointing decides when that logical stream commits/finalizes
- canceled in-flight turns reset the socket so stale provider events cannot leak into the next turn

That last point is the tradeoff. Cartesia gives us `context_id`, so multiple
TTS generations can be cleanly separated on one websocket. The STT events in
this adapter do not currently carry a per-turn ID, so a canceled STT turn cannot
be safely demultiplexed from the next one. Normal completion keeps the
connection. Cancellation resets it for correctness.

Why do this?

- connection setup is paid before the first turn, not during it
- normal turns avoid repeated TCP/TLS/websocket handshakes
- turn ownership remains explicit inside the pipeline
- stale transcripts are still prevented on interruption

A good low-latency system is not just fast on the happy path. It must also be
correct under interruption.

## 8. How barge-in works

Barge-in is not an extra feature. It is a core behavior.

In this codebase, a new confirmed `SpeechStart` while an older turn is still active means:

1. mark the old turn interrupted
2. cancel its context
3. stop its LLM/TTS/playback chain
4. create a new turn
5. seed the new turn with pre-roll audio

The important design choice is this:

The turn context is the unit of cancellation.

That means you do not need separate "stop LLM," "stop TTS," and "stop playback" control protocols in the orchestration code. You can still map cancellation to provider-specific controls, but the session logic stays simple because everything important hangs off a shared `context.Context`.

## 9. Why the channels are bounded

This is a realtime system. Unbounded channels are a trap.

If downstream stalls and upstream keeps writing, the system does not fail fast. It silently becomes delayed-time instead of real-time.

The code therefore uses bounded channels for:

- turn audio input
- transcript commits
- streamed phrases
- playback frames

When a realtime buffer fills up, the code prefers dropping stale backlog instead of pretending the system can catch up indefinitely.

This principle appears in multiple places because it is one of the core system design decisions.

## 10. Package-by-package reading order

If you want to master this codebase, read in this order:

1. `audio/frame.go`
2. `vad/vad.go`
3. `vad/energy.go`
4. `stt/stt.go`
5. `llm/llm.go`
6. `tts/tts.go`
7. `pipeline/preroll.go`
8. `pipeline/stabilizer.go`
9. `pipeline/chunker.go`
10. `pipeline/pipeline.go`
11. `session/manager.go`

That order mirrors the conceptual dependency graph.

## 11. The real adapters

- `stt/voxtral.go`
- `llm/mistral.go`
- `tts/cartesia.go`

These are intentionally thin wrappers. That is deliberate.

Provider wrappers should do as little orchestration as possible. Their job is:

- translate Go contracts to wire contracts
- keep connection lifecycle contained
- stream events in and out cleanly
- leave session policy to the pipeline

If provider packages start owning turn policy, your code becomes harder to reason about and harder to swap.

## 12. Latency budget

A useful target budget for this architecture is:

- VAD end-of-turn silence: `300-500ms`
- STT final after speech end: `150-300ms`
- LLM first token: `200-500ms`
- TTS first audio chunk: `100-300ms`

That normally gives you:

- strong path: under `1.5s`
- acceptable path: under `2.0s`

This repo is structured so you can measure those stages separately. The metrics collected in `pipeline.TurnMetrics` are there for that reason.

## 13. What to tune first

If the system feels slow, tune in this order:

1. `speechEnd` silence threshold in the VAD tracker
2. STT finalization latency
3. LLM first-token latency
4. TTS first-audio latency
5. phrase chunk size

Do not start by "optimizing Go code" blindly. Most voice latency lives in policy and network boundaries, not in local CPU time.

## 14. What I intentionally did not overbuild

This version is serious, but it is still disciplined.

I did not add:

- speculative LLM execution on unstable STT tails
- tool calling
- memory retrieval
- persistence
- reconnection state machines for every provider edge case
- a complex dependency injection framework

Why not?

Because those would increase surface area before the core voice loop was clean.

The right order is:

1. build the turn loop correctly
2. make cancellation correct
3. measure latency
4. only then add more capability

## 15. How to evolve this next

When you are ready for the next level, the best upgrades are:

1. Replace `EnergyVAD` with real Silero in live runs.
2. Use the real Mistral and Cartesia adapters with integration tests against captured payloads.
3. Add a richer transcript commit policy that can trigger early draft reasoning.
4. Add a websocket playback sink for telephony or browser delivery.
5. Add memory/tooling only after the realtime core remains stable.

That is how you keep a voice agent fast while making it more capable.

## 16. Final mental model

The most important thing to understand in this repository is this:

The hard problem is not "calling STT, LLM, and TTS."

The hard problem is making those stages behave like one coherent realtime instrument under:

- noisy input
- changing partial transcripts
- user interruption
- streaming output
- bounded latency budgets

That is what the pipeline package is really solving.
