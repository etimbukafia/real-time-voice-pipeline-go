# Go Concurrency Through This Pipeline

This guide teaches Go concurrency using this repository's actual runtime flow.

It is not a generic concurrency tutorial.

It answers concrete questions like:

- Which goroutines exist during a turn?
- Which goroutine owns the session state?
- What does each channel mean?
- What happens when the user interrupts the assistant?
- Why does cancellation work cleanly here?

Read this with:

1. `GO_CODEBASE_TEACHING_GUIDE.md`
2. `VOICE_PIPELINE_GUIDE.md`
3. `pipeline/pipeline.go`

## 1. The most important concurrency rule in this repo

The session loop owns the session state.

That means `pipeline.Run` is the authority for:

- current turn
- session state
- conversation history
- turn metrics
- whether a result is still relevant

Other goroutines do work.

They do not decide session policy.

That one rule removes a lot of concurrency complexity.

## 2. What "concurrency" means here

Concurrency in this repo does not mean "everything runs at once in a chaotic way."

It means:

- the main session loop keeps reacting to events
- STT can work while the loop keeps listening
- LLM/TTS can work while new user speech can still cancel them
- websocket readers/writers can block on I/O without freezing the rest of the system

This is structured concurrency around a realtime session.

## 3. The key goroutines in one session

At a high level, these goroutines can exist:

1. The main session goroutine in `pipeline.Run`
2. The audio source goroutine
3. A turn-local transcript bridge goroutine
4. A turn-local assistant response goroutine
5. A turn-local playback goroutine
6. A turn-local LLM-to-phrase goroutine
7. Provider-specific websocket reader/writer goroutines

Not all of them exist all the time.

The important thing is that each one has a narrow purpose.

## 4. The main session loop

The core loop waits on several inputs at once using `select`.

Conceptually:

```go
for {
    select {
    case <-ctx.Done():
    case result := <-transcriptResults:
    case progress := <-responseProgresses:
    case result := <-responseResults:
    case frame := <-frames:
    }
}
```

This is one of the central Go patterns in the repo.

`select` means:

"wait for whichever of these events becomes ready first."

That lets one goroutine coordinate many concurrent activities without polling.

## 5. The normal turn lifecycle

Here is the normal path for one clean turn.

### Sequence diagram

```text
AudioSource -> Pipeline.Run: audio frame
Pipeline.Run -> VAD StateTracker: Process(frame)
VAD StateTracker -> Pipeline.Run: SpeechStart
Pipeline.Run -> startTurn: create turn ctx + audio channel + STT stream
Pipeline.Run -> turn audio channel: pre-roll frames + current frame
AudioSource -> Pipeline.Run: more frames
Pipeline.Run -> turn audio channel: speech frames
VAD StateTracker -> Pipeline.Run: SpeechEnd
Pipeline.Run -> turn audio channel: close(channel)
STT -> Stabilizer: transcript partials/final
Stabilizer -> transcriptResults: final transcript
Pipeline.Run -> assistant goroutine: start response
assistant goroutine -> LLM: stream text
LLM pump -> TTS input channel: phrase chunks
TTS -> assistant goroutine: audio chunks
assistant goroutine -> playback goroutine: audio frames
assistant goroutine -> responseProgresses: first audio timestamp
assistant goroutine -> responseResults: final assistant result
Pipeline.Run -> session state: listening
```

## 6. What each channel means

This is the simplest way to make the repo feel less abstract.

### `frames`

Produced by the audio source.

Meaning:

"Here is the next incoming audio frame from the outside world."

### `current.audioIn`

Produced by the session loop.

Consumed by STT for one turn only.

Meaning:

"Here is audio that belongs to this specific user turn."

Closing it means:

"No more audio belongs to this turn; finalize now."

### `transcriptResults`

Produced by the turn-local transcript bridge.

Consumed by the session loop.

Meaning:

"This turn has a final transcript result for you."

### `responseProgresses`

Produced by the assistant response path.

Consumed by the session loop.

Meaning:

"Assistant speech has started producing audio."

### `responseResults`

Produced by the assistant response path.

Consumed by the session loop.

Meaning:

"The assistant-side work for this turn is done."

### `textToTTS`

Produced by the LLM pump goroutine.

Consumed by TTS.

Meaning:

"Here is the next phrase-sized chunk to synthesize."

### `playerInput`

Produced by the assistant response goroutine.

Consumed by the playback goroutine.

Meaning:

"Here is assistant audio ready for output."

## 7. Why the turn has its own context

Each turn gets its own `context.Context`.

That gives you a kill switch for the whole turn.

If the user interrupts, the code does not manually stop five separate things.

It cancels one turn context, and the rest of the turn-local goroutines observe it.

That is a powerful Go pattern:

use context to express the lifetime of a unit of work.

## 8. What happens during barge-in

Barge-in means:

the assistant is still working or speaking, and the user starts talking again.

This is one of the best places to study concurrency in the codebase.

### Sequence diagram

```text
assistant turn active
AudioSource -> Pipeline.Run: new frame
Pipeline.Run -> VAD StateTracker: Process(frame)
VAD StateTracker -> Pipeline.Run: SpeechStart
Pipeline.Run -> current turn metrics: mark interrupted
Pipeline.Run -> current turn context: cancel()
current turn context -> STT/LLM/TTS/playback goroutines: stop
Pipeline.Run -> startTurn: create new turn
Pipeline.Run -> new turn audio channel: pre-roll + current frame
new turn proceeds normally
```

### Why this is clean

Because the old turn does not need to "fight" the new one.

The old turn is simply declared stale.

That is the correct mental model:

- one turn is current
- the older one is no longer authoritative

## 9. Late results and why they are okay

In concurrent systems, you often get late results:

- STT finishes just after cancellation
- TTS emits one last chunk
- LLM completes while the user has already started a new turn

This repo handles that by centralizing relevance checks in the session loop.

The loop asks:

"Does this result still belong to the active turn?"

If not, it ignores it.

That is much safer than trying to guarantee no late result can ever happen.

In realtime systems, late results are normal.

What matters is whether stale results can still change state.

Here, they cannot.

## 10. Why `select` is such a big deal

If you only know one concurrency primitive from this guide, know `select`.

Why?

Because this pipeline is event-driven.

The system must react to whichever thing happens next:

- new audio arrives
- STT finishes
- assistant starts speaking
- assistant finishes
- session is canceled

`select` is the Go construct that naturally expresses that.

It is one of the reasons Go feels good for networked realtime systems.

## 11. Why channels are buffered in this repo

You will see channels like:

```go
make(chan transcriptResult, 8)
make(chan responseResult, 8)
make(chan string, 8)
```

Those buffers are not random.

They absorb small timing mismatches between producer and consumer.

Example:

- TTS emits a chunk slightly before playback is ready
- a transcript result arrives while the loop is processing another event

The buffer allows a small amount of decoupling.

But the buffers are still bounded.

That matters because this is a realtime system.

Unbounded buffering would quietly turn "realtime" into "lagging behind."

## 12. Closing channels: who does it?

A good Go design usually follows this rule:

the sender closes the channel.

That pattern appears throughout this repo.

Examples:

- the session loop closes `current.audioIn`
- the LLM pump closes `textToTTS`
- the reader side closes output channels when the producer is truly done

This matters because closing a channel from the wrong side is one of the easiest
ways to create panics or confusing bugs.

## 13. Why there is a mutex in TTS

Most of the repo avoids mutexes.

That is good.

The mutex in `tts/cartesia.go` exists for one concrete reason:

one websocket connection must not be written by multiple goroutines at the same time.

The writer goroutine and the cancellation path both want to write to the same
connection.

The mutex serializes those writes.

That is a good lesson:

use a mutex to protect one narrow shared resource when channel ownership alone is
not enough.

## 14. What is overlapped and why

This pipeline does not wait for each stage to finish completely before starting
the next.

Overlap examples:

- the main loop keeps listening while assistant work happens
- LLM output is chunked into phrases while the LLM is still streaming
- TTS can start before the full assistant response is complete
- playback can start before TTS is fully done

This overlap is where much of the latency improvement comes from.

Without overlap, the system would feel much slower.

## 15. Common beginner mistake: "Why not one big function?"

Because one big function would force one control flow to do too much:

- read audio
- detect turn boundaries
- wait for STT
- wait for LLM
- wait for TTS
- handle interruption

That is not one linear story.

It is several concurrent stories coordinated by one owner.

That is why the code is structured the way it is.

## 16. How to mentally debug concurrency here

When something feels confusing, ask these questions in order:

1. Which goroutine am I currently reading?
2. What does this goroutine own?
3. Which channel is it reading from?
4. Which channel is it writing to?
5. What makes it stop?
6. What happens if its context is canceled?

If you can answer those six questions, most of the code becomes much easier to follow.

## 17. A practical reading exercise

Try this:

1. Open `pipeline/pipeline.go`
2. Trace only the `SpeechStart` path
3. Then trace only the `SpeechEnd` path
4. Then trace only the `ctx.Done()` path
5. Then trace only the `responseResults` path

That is better than trying to understand the whole file at once.

Go concurrency becomes easier when you follow one event path at a time.

## 18. Final lesson

The point of concurrency here is not "parallelism for its own sake."

The point is:

to let the system stay responsive while several independent things are in flight.

This repo uses Go concurrency well when it does these three things:

- it makes ownership explicit
- it makes cancellation cheap
- it keeps the session loop in control

That is the real pattern to learn.
