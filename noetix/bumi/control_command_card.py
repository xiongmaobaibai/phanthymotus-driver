"""Read-only Card for Bumi high-level control-command observations."""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import threading
import time

from rclpy.node import Node
from std_msgs.msg import String

from bumi_control_monitor import CommandState, FRAME_PREFIX


class ControlCommandPlugin:
    PREFIX = "control_command"

    def __init__(self, config: dict, namespace: str, executor):
        stale = float(config.get("stale_after_s", 0.5))
        deadband = float(config.get("deadband", 0.05))
        if not math.isfinite(stale) or not 0.1 <= stale <= 10.0:
            raise ValueError("stale_after_s must be in [0.1, 10]")
        if not math.isfinite(deadband) or not 0.0 <= deadband < 1.0:
            raise ValueError("deadband must be in [0, 1)")
        self._state = CommandState(stale, deadband)
        self._lock = threading.RLock()
        self._stopping = threading.Event()
        self._thread = None
        self._proc = None
        self._last_heartbeat = None
        self._topic = f"/{namespace}/control/observed_command"
        self._events_topic = f"/{namespace}/control/observed_command_events"
        self._node = Node("bumi_control_command")
        self._publisher = self._node.create_publisher(String, self._topic, 10)
        self._events = self._node.create_publisher(String, self._events_topic, 10)
        self._node.create_timer(0.1, self._publish_state)
        executor.add_node(self._node)

    @staticmethod
    def _publish(publisher, payload):
        message = String()
        message.data = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        publisher.publish(message)

    @staticmethod
    def _terminate_process(proc):
        if proc is None or proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
                proc.wait(timeout=2.0)
            except ProcessLookupError:
                pass
        except ProcessLookupError:
            pass

    def _publish_state(self):
        timed_out = None
        with self._lock:
            now = time.monotonic()
            if (self._state.reader_ready and self._last_heartbeat is not None
                    and now - self._last_heartbeat > 3.0):
                self._state.unavailable("reader heartbeat timed out")
                timed_out = self._proc
            snapshot = self._state.snapshot(now)
        if timed_out is not None and timed_out.poll() is None:
            threading.Thread(target=self._terminate_process, args=(timed_out,),
                             name="bumi_control_timeout", daemon=True).start()
        self._publish(self._publisher, snapshot)

    def _frame(self, frame):
        kind = frame.get("kind")
        if kind in ("ready", "heartbeat"):
            with self._lock:
                self._last_heartbeat = time.monotonic()
                self._state.ready(frame["matched_publishers"])
        elif kind == "sample":
            with self._lock:
                if self._stopping.is_set() or not self._state.reader_ready:
                    return
                now = time.monotonic()
                command = self._state.observe(frame["sample"], now)
                received = min(now, frame["sample"].get("received_monotonic_s", now))
                fresh = now - received <= self._state.stale_after_s
            self._publish(self._events, {
                "state": "observed" if fresh else "stale", "fresh": fresh,
                "command": command if fresh else None,
            })
        elif kind == "error":
            with self._lock:
                self._state.unavailable(str(frame.get("error", "reader failed"))[:300])

    def _supervise(self):
        worker_path = os.path.join(os.path.dirname(__file__), "bumi_control_monitor.py")
        while not self._stopping.is_set():
            proc = None
            error = None
            with self._lock:
                if self._stopping.is_set():
                    break
                self._state.unavailable("starting")
            try:
                proc = subprocess.Popen(
                    [sys.executable, "-B", "-u", worker_path, "--worker"],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1,
                )
                with self._lock:
                    self._proc = proc
                if self._stopping.is_set():
                    proc.terminate()
                for line in proc.stdout:
                    if self._stopping.is_set():
                        break
                    if not line.startswith(FRAME_PREFIX):
                        continue
                    try:
                        self._frame(json.loads(line[len(FRAME_PREFIX):]))
                    except (ValueError, KeyError, TypeError, AttributeError) as exc:
                        print(f"[control_command] invalid reader frame: {exc}", flush=True)
                proc.wait()
            except Exception as exc:  # Keep a failed reader from escaping supervision.
                error = str(exc)[:300]
            finally:
                self._terminate_process(proc)
                if proc is not None and proc.stdout is not None:
                    proc.stdout.close()
                with self._lock:
                    if self._proc is proc:
                        self._proc = None
                    if not self._stopping.is_set():
                        code = proc.returncode if proc is not None else "not started"
                        worker_error = self._state.error if self._state.error != "starting" else None
                        detail = worker_error or error or f"reader exited ({code})"
                        self._state.unavailable(detail)
            if not self._stopping.is_set():
                self._stopping.wait(3.0)

    def start(self):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return not self._stopping.is_set()
            self._stopping.clear()
            self._state.unavailable("starting")
            self._thread = threading.Thread(target=self._supervise, name="bumi_control_command", daemon=True)
            self._thread.start()
        return True

    def stop(self):
        self._stopping.set()
        with self._lock:
            proc, thread = self._proc, self._thread
            self._state.unavailable("stopped")
        self._terminate_process(proc)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3.0)
        self._publish_state()

    def get_tool(self):
        return {
            "name": "control_command", "type": "sensor", "multiInstance": False,
            "description": (
                "只读观察 Bumi 的高层控制请求，报告前后、横移、转向控制量及原始动作码。"
                "来源可能是 App 或其他控制程序，不能证明命令已经执行；不能读取原始按键状态。"
            ),
            "inputSchema": {"type": "object", "properties": {
                "action": {"type": "string", "enum": ["info", "read", "start", "stop"]},
            }},
            "topic_out": [
                {"topic": self._topic, "format": "data/json"},
                {"topic": self._events_topic, "format": "data/json"},
            ],
        }

    def dispatch(self, action, args):
        if action == "start":
            return {"state": "starting" if self.start() else "stopping"}
        if action == "stop":
            self.stop()
            return {"state": "stopped"}
        if action in ("info", "read"):
            with self._lock:
                result = self._state.snapshot(time.monotonic())
            return {**result, "topic_out": self.get_tool()["topic_out"]}
        return None
