#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import datetime, timezone
import json
import struct
import sys
from typing import Any

try:
    from mistralai.client import Mistral
    from mistralai.client.models import (
        AudioFormat,
        RealtimeTranscriptionError,
        RealtimeTranscriptionSessionCreated,
        TranscriptionStreamDone,
        TranscriptionStreamTextDelta,
    )
    from mistralai.extra.realtime import UnknownRealtimeEvent
except ImportError:
    print(
        "python_mistral_worker error: install `mistralai[realtime]` in the Python environment used by VOICE_COACH_STT_PYTHON_EXE",
        file=sys.stderr,
        flush=True,
    )
    raise

CMD_START = 0x01
CMD_FRAME = 0x02
CMD_END = 0x03
CMD_CANCEL = 0x04
CMD_CLOSE = 0x05

EVT_PARTIAL = 0x11
EVT_FINAL = 0x12
EVT_ERROR = 0x13

_RETRYABLE_STATUS_CODES = frozenset({408, 409, 425, 429, 500, 502, 503, 504})
_RETRYABLE_TEXT_MARKERS = (
    "rate limit",
    "too many requests",
    "quota",
    "temporarily unavailable",
    "service unavailable",
    "timed out",
    "timeout",
    "connection reset",
    "connection closed",
    "try again later",
)


def write_event(event_type: int, payload: dict[str, Any]) -> None:
    data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    stdout = sys.stdout.buffer
    stdout.write(bytes([event_type]))
    stdout.write(struct.pack("<I", len(data)))
    stdout.write(data)
    stdout.flush()


def emit_final(text: str, *, confidence: float = 0.0, timestamp_ms: int = 0, timestamp: str = "", start_ms: int = 0, end_ms: int = 0) -> None:
    cleaned = text.strip()
    if not cleaned:
        return
    write_event(
        EVT_FINAL,
        {
            "text": cleaned,
            "confidence": float(confidence or 0.0),
            "timestamp": event_timestamp(timestamp_ms, timestamp),
            "is_final": True,
            "start_ms": int(start_ms or 0),
            "end_ms": int(end_ms or 0),
        },
    )


def read_exact(stream, size: int) -> bytes:
    data = stream.read(size)
    if data is None or len(data) != size:
        raise EOFError(f"expected {size} bytes, got {0 if data is None else len(data)}")
    return data


def should_retry_provider_error(exc: BaseException) -> bool:
    status_code = None
    for attr in ("status_code", "status", "http_status", "code"):
        value = getattr(exc, attr, None)
        if isinstance(value, int) and 100 <= value <= 599:
            status_code = value
            break
    if status_code in _RETRYABLE_STATUS_CODES:
        return True
    message = str(exc).lower()
    return any(marker in message for marker in _RETRYABLE_TEXT_MARKERS)


def retry_delay_seconds(attempt: int) -> float:
    return min(2.0 * (2 ** max(0, attempt - 1)), 10.0)


def event_timestamp(timestamp_ms: int | None, timestamp: str | None) -> str:
    if timestamp_ms:
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone().isoformat()
    if timestamp:
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone().isoformat()
        except ValueError:
            pass
    return datetime.now().astimezone().isoformat()


class RealtimeTranscriber:
    def __init__(self, api_key: str, model: str, server_url: str, target_delay_ms: int) -> None:
        client_kwargs: dict[str, Any] = {"api_key": api_key}
        if server_url:
            client_kwargs["server_url"] = server_url
        self._client = Mistral(**client_kwargs)
        self._model = model
        self._audio_format = AudioFormat(encoding="pcm_s16le", sample_rate=16000)
        self._target_delay_ms = max(0, int(target_delay_ms))
        self._audio_queue: asyncio.Queue[tuple[int, bytes] | None] | None = None
        self._replay_buffer: deque[tuple[int, bytes]] = deque(maxlen=250)
        self._frame_index = 0
        self._turn_task: asyncio.Task[None] | None = None

    async def start_turn(self) -> None:
        await self.cancel_turn()
        self._audio_queue = asyncio.Queue()
        self._replay_buffer = deque(maxlen=250)
        self._frame_index = 0
        self._turn_task = asyncio.create_task(self._run_turn(), name="python-mistral-turn")

    async def push_frame(self, frame: bytes) -> None:
        if self._audio_queue is None:
            return
        item = (self._frame_index, frame)
        self._frame_index += 1
        self._replay_buffer.append(item)
        await self._audio_queue.put(item)

    async def end_turn(self) -> None:
        if self._audio_queue is not None:
            await self._audio_queue.put(None)

    async def cancel_turn(self) -> None:
        task = self._turn_task
        self._turn_task = None
        self._audio_queue = None
        self._replay_buffer = deque(maxlen=250)
        self._frame_index = 0
        if task is None:
            return
        task.cancel()
        with suppress(BaseException):
            await task

    async def close(self) -> None:
        await self.cancel_turn()
        close = getattr(self._client, "close", None)
        if close is not None:
            maybe = close()
            if asyncio.iscoroutine(maybe):
                await maybe

    async def _run_turn(self) -> None:
        emitted = False
        attempt = 0
        replay = False
        last_confidence = 0.0
        last_timestamp_ms = 0
        last_timestamp = ""
        last_start_ms = 0
        last_end_ms = 0
        while True:
            full_text = ""
            try:
                kwargs: dict[str, Any] = {
                    "audio_stream": self._iter_audio_bytes(self._attempt_stream(replay=replay)),
                    "model": self._model,
                    "audio_format": self._audio_format,
                }
                if self._target_delay_ms > 0:
                    kwargs["target_streaming_delay_ms"] = self._target_delay_ms

                async for event in self._client.audio.realtime.transcribe_stream(**kwargs):
                    if isinstance(event, RealtimeTranscriptionSessionCreated):
                        continue
                    if isinstance(event, TranscriptionStreamTextDelta):
                        full_text += getattr(event, "text", "")
                        emitted = True
                        last_confidence = float(getattr(event, "confidence", 0.0) or 0.0)
                        last_timestamp_ms = int(getattr(event, "timestamp_ms", 0) or 0)
                        last_timestamp = getattr(event, "timestamp", "") or ""
                        last_start_ms = int(getattr(event, "start_ms", 0) or 0)
                        last_end_ms = int(getattr(event, "end_ms", 0) or 0)
                        write_event(
                            EVT_PARTIAL,
                            {
                                "text": full_text,
                                "confidence": last_confidence,
                                "timestamp": event_timestamp(
                                    last_timestamp_ms,
                                    last_timestamp,
                                ),
                                "is_final": False,
                                "start_ms": last_start_ms,
                                "end_ms": last_end_ms,
                            },
                        )
                        continue
                    if isinstance(event, TranscriptionStreamDone):
                        emit_final(
                            full_text,
                            confidence=float(getattr(event, "confidence", 0.0) or 0.0),
                            timestamp_ms=int(getattr(event, "timestamp_ms", 0) or 0),
                            timestamp=getattr(event, "timestamp", "") or "",
                            start_ms=int(getattr(event, "start_ms", 0) or 0),
                            end_ms=int(getattr(event, "end_ms", 0) or 0),
                        )
                        return
                    if isinstance(event, RealtimeTranscriptionError):
                        raise RuntimeError(f"Mistral realtime transcription error: {event}")
                    if isinstance(event, UnknownRealtimeEvent):
                        continue
                if full_text.strip():
                    emit_final(
                        full_text,
                        confidence=last_confidence,
                        timestamp_ms=last_timestamp_ms,
                        timestamp=last_timestamp,
                        start_ms=last_start_ms,
                        end_ms=last_end_ms,
                    )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                replay = True
                if full_text.strip():
                    emit_final(
                        full_text,
                        confidence=last_confidence,
                        timestamp_ms=last_timestamp_ms,
                        timestamp=last_timestamp,
                        start_ms=last_start_ms,
                        end_ms=last_end_ms,
                    )
                    return
                if emitted or attempt >= 5 or not should_retry_provider_error(exc):
                    write_event(EVT_ERROR, {"message": str(exc)})
                    return
                await asyncio.sleep(retry_delay_seconds(attempt))

    async def _iter_audio_bytes(self, audio_stream: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        async for frame in audio_stream:
            if frame and len(frame) % 2 == 0:
                yield frame

    async def _attempt_stream(self, *, replay: bool) -> AsyncIterator[bytes]:
        if self._audio_queue is None:
            return

        replay_cutoff = -1
        if replay:
            buffered_frames = list(self._replay_buffer)
            if buffered_frames:
                replay_cutoff = buffered_frames[-1][0]
            for _, frame in buffered_frames:
                yield frame

        queue = self._audio_queue
        while True:
            item = await queue.get()
            if item is None:
                await queue.put(None)
                return
            frame_index, frame = item
            if frame_index <= replay_cutoff:
                continue
            yield frame


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--server-url", default="https://api.mistral.ai")
    parser.add_argument("--target-delay-ms", type=int, default=0)
    args = parser.parse_args()

    worker = RealtimeTranscriber(
        api_key=args.api_key,
        model=args.model,
        server_url=args.server_url,
        target_delay_ms=args.target_delay_ms,
    )

    stdout = sys.stdout.buffer
    stdout.write(b"MRDY")
    stdout.flush()

    loop = asyncio.get_running_loop()
    stdin = sys.stdin.buffer

    try:
        while True:
            cmd_raw = await loop.run_in_executor(None, stdin.read, 1)
            if not cmd_raw:
                return
            cmd = cmd_raw[0]

            if cmd == CMD_START:
                await worker.start_turn()
                continue
            if cmd == CMD_FRAME:
                length = struct.unpack("<I", read_exact(stdin, 4))[0]
                payload = read_exact(stdin, length)
                await worker.push_frame(payload)
                continue
            if cmd == CMD_END:
                await worker.end_turn()
                continue
            if cmd == CMD_CANCEL:
                await worker.cancel_turn()
                continue
            if cmd == CMD_CLOSE:
                return

            raise ValueError(f"unknown command byte {cmd:#x}")
    finally:
        await worker.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"python_mistral_worker error: {exc}", file=sys.stderr, flush=True)
        raise
