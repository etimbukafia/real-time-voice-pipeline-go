# Go Codebase Teaching Guide

This document exists for one reason:

to help you learn Go from this codebase without forcing every source file to
carry tutorial-sized comments.

The code comments should explain the hard local decisions. This guide explains
the bigger Go ideas that repeat across the project.

Read this together with:

1. `VOICE_PIPELINE_GUIDE.md`
2. `pipeline/pipeline.go`
3. `stt/voxtral.go`
4. `tts/cartesia.go`
5. `llm/mistral.go`

## 1. The main Go mindset in this repo

If you come from Python, the biggest shift is this:

you do not build this kind of system by passing objects around and letting
everything call everything else.

In this repo, the shape is:

- small interfaces
- explicit ownership
- channels for handoff
- `context.Context` for cancellation
- one goroutine owning critical mutable state

That is the core pattern.

## 2. Interfaces: why they exist here

You asked for a simple provider model, and that is exactly the right instinct.

The interfaces here are intentionally small:

- `audio.AudioSource`
- `vad.VAD`
- `stt.STT`
- `llm.Client`
- `tts.Engine`
- `playback.Player`

These interfaces are not "abstraction for abstraction's sake."

They exist so the pipeline can say:

"I need something that produces audio frames"

instead of:

"I need this exact websocket client, with this exact auth flow, with this exact provider payload."

That separation is what keeps `pipeline.Run` readable.

The rule to learn is:

use interfaces at subsystem boundaries, not everywhere.

That is what this repo does.

## 3. The composition root

Study `cmd/voice-pipeline/app_config.go`.

That file is the composition root.

A composition root is the place where:

- concrete implementations are chosen
- configuration is read
- real providers are assembled
- interfaces are satisfied

After that point, the rest of the code should mostly stop caring about which
provider was chosen.

This is one of the cleanest Go design habits you can build early.

## 4. Why `context.Context` matters so much

If you only learn one Go concept deeply from this project, learn `context`.

In this repo, a turn context means:

- STT should stop reading/writing
- LLM should stop generating
- TTS should stop synthesizing
- playback should stop sending audio

One signal shuts down the entire stale turn.

That is exactly what `context.Context` is good at:

propagating cancellation and deadlines through a tree of work.

This is much cleaner than inventing your own custom stop flags everywhere.

## 5. Why the pipeline loop is written as one owner

Study `pipeline.Run`.

The session loop owns:

- current turn
- session state
- conversation history
- timing/metrics

This matters because shared mutable state is where concurrency bugs grow.

A common beginner mistake is:

- goroutine A changes session state
- goroutine B changes turn state
- goroutine C updates metrics

That quickly becomes hard to reason about.

This repo instead follows a stronger rule:

- helper goroutines do background work
- the main loop makes session decisions

That is much easier to understand and maintain.

## 6. Channels: what they mean in this repo

A channel here usually means:

"I am handing ownership of an event or unit of work to another part of the system."

Examples:

- audio frames go into a turn-local STT input channel
- transcript results come back through a result channel
- assistant progress comes back through another channel

Important beginner rule:

channels are not just queues.

They are contracts about who sends, who receives, and who is responsible for
closing them.

In good Go code, you should usually be able to answer:

- Who writes to this channel?
- Who reads from it?
- Who closes it?
- What does closing it mean?

If those answers are blurry, the design is weak.

## 7. Closing a channel does not mean "error"

This is an important Go idea.

In this repo, closing a channel usually means:

"no more values are coming"

not:

"something failed"

For example:

- closing the turn audio channel tells STT that the utterance is complete
- closing an output stream tells downstream code that the producer is done

Errors are usually carried separately, not by "mysterious channel closing."

## 8. Why goroutines are used where they are used

This repo does not spawn goroutines everywhere.

That is deliberate.

Good Go is not "add goroutines whenever possible."

Good Go is:

add a goroutine only when there is a genuine concurrent responsibility.

Examples from this repo:

- one goroutine reads a websocket stream
- one goroutine writes a websocket stream
- one goroutine runs assistant response generation while the main loop keeps listening
- one goroutine runs playback because playback can block

The question to ask is:

"What must continue progressing independently of this current call stack?"

That is when a goroutine makes sense.

## 9. Why mutexes are rare here

This is also deliberate.

A beginner often sees concurrency and thinks mutex first.

But this repo tries to avoid shared mutable state in the first place.

That is why most coordination is done through:

- channel ownership
- single-loop ownership
- context cancellation

The mutex in `tts/cartesia.go` exists for a specific reason:

the websocket connection must not be written from multiple goroutines at the
same time.

That is a narrow, concrete correctness rule.

That is a good use of a mutex.

## 10. Why helper functions are small

A lot of functions here are short and narrowly scoped.

That is not accidental.

In Go, small functions are especially useful because:

- error handling is explicit
- control flow is easier to scan
- responsibilities stay obvious

For example:

- `buildSTT`
- `buildLLM`
- `buildTTS`
- `sendFrameLatest`
- `pumpLLMToPhrases`

Each one does one thing.

That makes the code easier to study.

## 11. Error handling in Go versus Python

If you come from Python, explicit error returns can feel noisy at first.

But there is a real upside:

the failure paths are visible in normal control flow.

You do not have to mentally guess which functions might throw exceptions.

In this repo, the common pattern is:

```go
thing, err := buildThing(...)
if err != nil {
	return nil, err
}
```

That may look repetitive, but it makes the program honest.

## 12. Why the provider wrappers are thin

The provider packages should not own session policy.

That means:

- `stt/voxtral.go` should translate websocket events into transcripts
- `llm/mistral.go` should translate SSE into text chunks
- `tts/cartesia.go` should translate websocket events into audio chunks

They should not decide:

- when a turn starts
- when a turn ends
- how interruption works
- what counts as stable transcript

That belongs in the pipeline.

This separation is one of the strongest design choices in the repo.

## 13. Why this repo uses structs instead of "bags of values"

In Python, you might use dicts or loose objects very naturally.

In Go, named structs are often better because they:

- document intent
- make field meaning explicit
- give you compiler help

Examples:

- `pipeline.Config`
- `pipeline.TurnMetrics`
- `llm.Request`
- `tts.Request`
- `stt.Transcript`

This makes the code more discoverable while you are learning.

## 14. How to read a Go function in this repo

A good reading order is:

1. read the function signature
2. identify owned inputs
3. identify outputs
4. identify cancellation path
5. identify goroutines started inside it
6. identify who closes channels

If you do that consistently, concurrency-heavy Go becomes much easier to follow.

## 15. The most important beginner lesson from this codebase

The best thing to learn here is not just syntax.

It is this design habit:

make ownership obvious.

Ownership of:

- state
- channels
- goroutines
- cancellation
- provider connections

When ownership is clear, the code feels simple.

When ownership is unclear, concurrency becomes scary.

## 16. What to study next in this repo

After reading this guide, go back to these files in order:

1. `cmd/voice-pipeline/app_config.go`
2. `pipeline/pipeline.go`
3. `pipeline/stabilizer.go`
4. `stt/voxtral.go`
5. `llm/mistral.go`
6. `tts/cartesia.go`

At that point, the code should feel much less like "magic goroutines" and much
more like explicit, structured dataflow.
