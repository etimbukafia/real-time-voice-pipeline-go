#!/usr/bin/env python3

import argparse
import struct
import sys

import numpy as np
import onnxruntime as ort


CMD_FRAME = 0x01
CMD_RESET = 0x02
CMD_CLOSE = 0x03

RESP_FALSE = b"\x00"
RESP_TRUE = b"\x01"
RESP_ACK = b"\x02"


def read_exact(stream, size):
    data = stream.read(size)
    if data is None or len(data) != size:
        raise EOFError(f"expected {size} bytes, got {0 if data is None else len(data)}")
    return data


class SileroWorker:
    def __init__(self, model_path: str, sample_rate: int, threshold: float):
        if sample_rate not in (8000, 16000):
            raise ValueError(f"unsupported sample rate {sample_rate}")

        opts = ort.SessionOptions()
        opts.intra_op_num_threads = 1
        opts.inter_op_num_threads = 1
        opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL

        self.session = ort.InferenceSession(
            model_path,
            sess_options=opts,
            providers=["CPUExecutionProvider"],
        )
        self.sample_rate = sample_rate
        self.threshold = threshold
        self.window_size = 512 if sample_rate == 16000 else 256
        self.context_len = 64
        self.reset()

    def reset(self):
        self.state = np.zeros((2, 1, 128), dtype=np.float32)
        self.ctx = np.zeros((self.context_len,), dtype=np.float32)
        self.window = np.empty((0,), dtype=np.float32)
        self.curr_sample = 0
        self.last_decision = False

    def process(self, pcm_bytes: bytes) -> bool:
        if len(pcm_bytes) % 2 != 0:
            raise ValueError(f"malformed PCM16 frame length {len(pcm_bytes)}")

        if not pcm_bytes:
            return False

        samples = np.frombuffer(pcm_bytes, dtype="<i2").astype(np.float32) / 32768.0
        if self.window.size == 0:
            self.window = samples
        else:
            self.window = np.concatenate((self.window, samples))
        if self.window.size > self.window_size:
            self.window = self.window[-self.window_size :]

        if self.window.size < self.window_size:
            return self.last_decision

        prob = self._infer_window(self.window)
        self.last_decision = prob >= self.threshold
        return self.last_decision

    def _infer_window(self, samples: np.ndarray) -> float:
        if self.curr_sample > 0:
            pcm = np.concatenate((self.ctx, samples)).astype(np.float32, copy=False)
        else:
            pcm = samples.astype(np.float32, copy=False)

        self.ctx = samples[-self.context_len :].copy()
        self.curr_sample += samples.size

        outputs = self.session.run(
            ["output", "stateN"],
            {
                "input": pcm.reshape(1, -1),
                "state": self.state,
                "sr": np.array([self.sample_rate], dtype=np.int64),
            },
        )
        prob = float(np.asarray(outputs[0]).reshape(-1)[0])
        self.state = np.asarray(outputs[1], dtype=np.float32)
        return prob


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--sample-rate", type=int, default=16000)
    parser.add_argument("--threshold", type=float, default=0.5)
    args = parser.parse_args()

    worker = SileroWorker(args.model, args.sample_rate, args.threshold)
    stdout = sys.stdout.buffer
    stdin = sys.stdin.buffer

    stdout.write(b"RDY1")
    stdout.flush()

    while True:
        cmd_raw = stdin.read(1)
        if not cmd_raw:
            return
        cmd = cmd_raw[0]

        if cmd == CMD_FRAME:
            length = struct.unpack("<I", read_exact(stdin, 4))[0]
            payload = read_exact(stdin, length)
            is_speech = worker.process(payload)
            stdout.write(RESP_TRUE if is_speech else RESP_FALSE)
            stdout.flush()
            continue

        if cmd == CMD_RESET:
            worker.reset()
            stdout.write(RESP_ACK)
            stdout.flush()
            continue

        if cmd == CMD_CLOSE:
            return

        raise ValueError(f"unknown command byte {cmd:#x}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"python_silero_worker error: {exc}", file=sys.stderr, flush=True)
        raise
