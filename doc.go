// Package voicepipeline is the importable root of the reusable real-time voice
// pipeline module.
//
// Most applications consume the focused subpackages directly: audio, vad, stt,
// llm, tts, playback, pipeline, and session. The root package exists to make
// the module self-documenting and importable while keeping product wiring out
// of the shared runtime.
package voicepipeline
