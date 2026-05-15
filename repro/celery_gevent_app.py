"""Celery app pointed at the Azure Service Bus emulator.

Used by repro/celery_gevent_worker.py (worker, runs under -P gevent)
and repro/celery_gevent_producer.py (sends tasks, verifies results).

Broker URL points at the emulator. use_lock_renewal is enabled with a
short peek_lock so the renewer must fire for tasks that sleep beyond
it. If the renewer fails under gevent, the broker will redeliver the
message and the task will execute more than once (caught by an
idempotency counter written to /tmp).
"""
import json
import os
import time
from pathlib import Path

from celery import Celery

BROKER = (
    "azureservicebus://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost.net"
)
EMU = (
    "Endpoint=sb://localhost;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
)

PEEK_LOCK_SECONDS = 10
MAX_LOCK_RENEWAL_DURATION = 120

EXEC_LOG = Path("/tmp/celery_gevent_exec_log.jsonl")
EXEC_LOG.touch()


app = Celery(
    "celery_gevent_test",
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


# Monkey-patch the channel to attach to the emulator. Done at import
# time so it's in place when the worker boots its channels.
#
# Two patches:
#   1. _try_parse_connection_string: route to the local emulator AMQP
#      port, not Azure cloud.
#   2. queue_mgmt_service: stub it out. The emulator doesn't expose an
#      HTTPS management endpoint, but the queues are pre-declared in
#      repro/Config.json. _new_queue's create_queue call will hit
#      connection refused on :443 without this; ResourceExistsError
#      catch doesn't help (different exception class).
from unittest.mock import MagicMock  # noqa: E402
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
_asb.Channel.queue_mgmt_service.__set_name__(_asb.Channel, "queue_mgmt_service")


def _record(task_name, task_id, event, duration=None, extra=None):
    rec = {
        "ts": time.time(),
        "pid": os.getpid(),
        "task": task_name,
        "task_id": task_id,
        "event": event,
    }
    if duration is not None:
        rec["duration"] = duration
    if extra:
        rec.update(extra)
    with EXEC_LOG.open("a") as f:
        f.write(json.dumps(rec) + "\n")


@app.task(bind=True, name="long_task")
def long_task(self, duration: float, label: str):
    """Sleep `duration` seconds. Idempotent: records start/end with
    task_id so the producer can detect re-execution from broker
    redelivery."""
    import gevent
    _record("long_task", self.request.id, "start",
            duration=duration, extra={"label": label})
    gevent.sleep(duration)
    _record("long_task", self.request.id, "end",
            duration=duration, extra={"label": label})
    return {"label": label, "duration": duration}
