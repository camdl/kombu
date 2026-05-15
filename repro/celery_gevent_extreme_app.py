"""Extreme stress version of celery_gevent_app.

Pushes harder than celery_gevent_app:
  - peek_lock=5s (minimum Azure allows; forces aggressive renewal)
  - durations from 1s to 90s, weighted toward long handlers
  - separate idempotency log AND renewal log
  - patched ServiceBusReceiver.renew_message_lock writes to a file
    so the producer can correlate renewals to task_id

Failure tasks: a subset of tasks raise on first execution to verify
the broker redelivers (and the renewer handles the re-registration).
"""
import json
import os
import time
from pathlib import Path
from unittest.mock import MagicMock

from celery import Celery

from azure.servicebus import ServiceBusReceiver


BROKER = (
    "azureservicebus://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost.net"
)
EMU = (
    "Endpoint=sb://localhost;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
)

PEEK_LOCK_SECONDS = 10  # emulator minimum honored
MAX_LOCK_RENEWAL_DURATION = 600  # 10 minutes; well above max task duration
CONCURRENCY = 50

EXEC_LOG = Path("/tmp/celery_gevent_extreme_exec.jsonl")
RENEW_LOG = Path("/tmp/celery_gevent_extreme_renew.jsonl")
HEARTBEAT_LOG = Path("/tmp/celery_gevent_extreme_heartbeat.jsonl")


for p in (EXEC_LOG, RENEW_LOG, HEARTBEAT_LOG):
    p.touch()


app = Celery(
    "celery_gevent_extreme",
    broker=BROKER,
    backend=None,
    broker_transport_options={
        "peek_lock_seconds": PEEK_LOCK_SECONDS,
        "use_lock_renewal": True,
        "max_lock_renewal_duration": MAX_LOCK_RENEWAL_DURATION,
        "wait_time_seconds": 1,
    },
)
app.conf.task_acks_late = True
app.conf.task_reject_on_worker_lost = True
app.conf.worker_prefetch_multiplier = 1
app.conf.task_default_queue = "celery-extreme"
# Surface unhandled task errors immediately rather than waiting on retry
# backoff (we want to see redelivery behaviour deterministically).
app.conf.task_default_retry_delay = 1


# Route the SDK to the emulator AMQP port, stub the management API
# (emulator has none).
import kombu.transport.azureservicebus as _asb  # noqa: E402
from kombu.utils.objects import cached_property  # noqa: E402

_orig_try_parse = _asb.Channel._try_parse_connection_string


def _patched_try_parse(self):
    _orig_try_parse(self)
    self._connection_string = EMU
    self._namespace = "localhost"


_asb.Channel._try_parse_connection_string = _patched_try_parse


def _stub_mgmt(self):
    m = MagicMock()
    m.create_queue.return_value = None
    m.delete_queue.return_value = None
    return m


_asb.Channel.queue_mgmt_service = cached_property(_stub_mgmt)
_asb.Channel.queue_mgmt_service.__set_name__(
    _asb.Channel, "queue_mgmt_service")


# Instrument renewals. Logs each renewal RPC to RENEW_LOG so the
# producer can correlate them with handler durations.
_orig_renew = ServiceBusReceiver.renew_message_lock


def _counted_renew(self, message, **kwargs):
    try:
        body = message.body if isinstance(message.body, bytes) else (
            b''.join(message.body))
        body_decoded = body.decode("utf-8", errors="ignore")
    except Exception:
        body_decoded = ""
    with RENEW_LOG.open("a") as f:
        f.write(json.dumps({
            "ts": time.time(),
            "pid": os.getpid(),
            "msg_body_head": body_decoded[:120],
            "msg_id": id(message),
        }) + "\n")
    return _orig_renew(self, message, **kwargs)


ServiceBusReceiver.renew_message_lock = _counted_renew


def _record(task_name, task_id, event, **extra):
    rec = {
        "ts": time.time(),
        "pid": os.getpid(),
        "task": task_name,
        "task_id": task_id,
        "event": event,
    }
    rec.update(extra)
    with EXEC_LOG.open("a") as f:
        f.write(json.dumps(rec) + "\n")


@app.task(bind=True, name="extreme_task", max_retries=3,
          default_retry_delay=1)
def extreme_task(self, duration: float, label: str,
                 fail_first_run: bool = False):
    """Sleep `duration` seconds (via gevent.sleep so the loop runs).

    If fail_first_run=True and this is the first delivery, raise to
    force broker redelivery. Records start/end per execution so the
    producer can detect over-delivery."""
    import gevent
    _record("extreme_task", self.request.id, "start",
            duration=duration, label=label,
            retries=self.request.retries,
            delivery_count=self.request.delivery_info.get(
                "delivery_count", None)
            if isinstance(self.request.delivery_info, dict) else None)
    if fail_first_run and self.request.retries == 0:
        gevent.sleep(min(duration, 1.0))
        _record("extreme_task", self.request.id, "raised",
                label=label)
        raise self.retry(exc=RuntimeError(
            f"intentional failure on first run {label}"))
    gevent.sleep(duration)
    _record("extreme_task", self.request.id, "end",
            duration=duration, label=label,
            retries=self.request.retries)
    return {"label": label, "duration": duration,
            "retries": self.request.retries}


@app.task(bind=True, name="heartbeat_task")
def heartbeat_task(self):
    """Runs forever as long as it's scheduled. Records gevent.sleep
    cadence to detect event-loop blocking caused by other handlers /
    the renewer. We re-issue from the producer side periodically."""
    import gevent
    last = time.monotonic()
    samples = []
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        gevent.sleep(0.1)
        now = time.monotonic()
        samples.append(now - last)
        last = now
    if samples:
        max_gap = max(samples)
        avg = sum(samples) / len(samples)
        with HEARTBEAT_LOG.open("a") as f:
            f.write(json.dumps({
                "ts": time.time(),
                "pid": os.getpid(),
                "n": len(samples),
                "avg_ms": avg * 1000,
                "max_ms": max_gap * 1000,
            }) + "\n")
    return {"n": len(samples), "max_ms": max(samples) * 1000 if samples
            else 0}
