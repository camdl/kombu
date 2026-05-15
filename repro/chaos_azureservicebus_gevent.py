"""Chaos test for the azureservicebus transport, under gevent.

Same shape as ``chaos_azureservicebus.py`` but with gevent
monkey-patching applied before anything else is imported. This
exercises the path that surfaced the original bug report at
celery/kombu#2542 (gevent users seeing the cache-sharing failure
mode).

Background: ``AutoLockRenewer`` spawns its own renewal thread. Under
gevent monkey-patching, that thread becomes a greenlet running on the
gevent hub, which previously interacted badly with the class-level
Channel cache (sibling Channel close from one greenlet evicted
receivers held by another). With ``_queue_cache`` and ``_renewer``
both scoped to ``Transport`` (per-Connection), each worker greenlet
should see its own state.

Run after starting the emulator:

    docker compose -f repro/docker-compose.yml up -d
    python repro/chaos_azureservicebus_gevent.py

This file is not committed; local scaffolding only.
"""
from __future__ import annotations

from gevent import monkey
monkey.patch_all()  # noqa: E402  - must run before any other import

import random  # noqa: E402
import time  # noqa: E402
import traceback  # noqa: E402
import threading  # noqa: E402
from collections import Counter  # noqa: E402

import gevent  # noqa: E402
from gevent.event import Event as GEvent  # noqa: E402

import azure.servicebus.exceptions  # noqa: E402
from kombu import Connection  # noqa: E402

URL = "azureservicebus://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost.net"
EMU = ("Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
       "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
QUEUE = "test-renewal"
DURATION_SECONDS = 60
NUM_WORKERS = 4
PRODUCER_RATE_PER_SEC = 6
PEEK_LOCK_SECONDS = 10
MAX_RENEWAL_SECONDS = 120
HANDLER_SLEEP_RANGE = (0.2, 18.0)
EIGHT_EIGHT_SEVEN_EIGHT_PATTERNS = ("send_messages", "create_sender_link")


def _attach_emulator(channel) -> None:
    channel._connection_string = EMU
    channel._namespace = "localhost"


def make_connection(*, use_renewal: bool = True) -> Connection:
    opts = {
        "peek_lock_seconds": PEEK_LOCK_SECONDS,
        "wait_time_seconds": 1,
        "polling_interval": 0.1,
    }
    if use_renewal:
        opts["use_lock_renewal"] = True
        opts["max_lock_renewal_duration"] = MAX_RENEWAL_SECONDS
    return Connection(URL, transport_options=opts)


class Stats:
    def __init__(self) -> None:
        self.produced = 0
        self.acked = 0
        self.lock_lost = 0
        self.regression_attr_errors = 0
        self.sdk_attr_errors = 0
        self.other_errors: Counter[str] = Counter()

    def bump(self, key: str) -> None:
        setattr(self, key, getattr(self, key) + 1)

    def record_error(self, exc: BaseException) -> None:
        if isinstance(exc, AttributeError):
            tb = "".join(traceback.format_exception(
                type(exc), exc, exc.__traceback__))
            if any(p in tb for p in EIGHT_EIGHT_SEVEN_EIGHT_PATTERNS):
                self.regression_attr_errors += 1
            else:
                self.sdk_attr_errors += 1
        self.other_errors[type(exc).__name__] += 1


def producer_loop(stop: GEvent, stats: Stats) -> None:
    conn = make_connection(use_renewal=False)
    try:
        channel = conn.channel()
        _attach_emulator(channel)
        interval = 1.0 / PRODUCER_RATE_PER_SEC
        i = 0
        while not stop.is_set():
            try:
                channel._put(
                    QUEUE, {"body": f"msg-{i}", "properties": {}})
                stats.bump("produced")
            except Exception as exc:
                stats.record_error(exc)
            i += 1
            stop.wait(interval)
    finally:
        try:
            conn.release()
        except Exception:
            pass


def worker_loop(stop: GEvent, stats: Stats,
                renewer_observed: list[object]) -> None:
    conn = make_connection(use_renewal=True)
    try:
        channel = conn.channel()
        _attach_emulator(channel)
        receiver = channel._get_asb_receiver(QUEUE).receiver
        renewer_observed.append(conn.transport._renewer)
        while not stop.is_set():
            try:
                messages = receiver.receive_messages(
                    max_message_count=2, max_wait_time=2)
            except Exception as exc:
                stats.record_error(exc)
                continue
            for msg in messages:
                stop.wait(random.uniform(*HANDLER_SLEEP_RANGE))
                if stop.is_set():
                    break
                try:
                    receiver.complete_message(msg)
                    stats.bump("acked")
                except azure.servicebus.exceptions.MessageLockLostError:
                    stats.bump("lock_lost")
                except Exception as exc:
                    stats.record_error(exc)
    finally:
        try:
            conn.release()
        except Exception:
            pass


def churn_loop(stop: GEvent, stats: Stats) -> None:
    while not stop.is_set():
        try:
            c = make_connection(use_renewal=True)
            ch = c.channel()
            _attach_emulator(ch)
            ch._put(QUEUE, {"body": "churn", "properties": {}})
            c.release()
        except Exception as exc:
            stats.record_error(exc)
        stop.wait(2.0)


def main() -> int:
    is_patched = monkey.is_module_patched("threading")
    print(
        f"chaos-gevent run: threading_monkey_patched={is_patched} "
        f"duration={DURATION_SECONDS}s workers={NUM_WORKERS} "
        f"producer_rate={PRODUCER_RATE_PER_SEC}/s "
        f"handler_sleep={HANDLER_SLEEP_RANGE} "
        f"peek_lock={PEEK_LOCK_SECONDS}s renewal={MAX_RENEWAL_SECONDS}s"
    )
    if not is_patched:
        print("FAIL: gevent monkey-patch was not applied; aborting")
        return 1
    stats = Stats()
    baseline_threads = threading.active_count()
    stop = GEvent()
    renewer_observed: list[object] = []

    greenlets = [
        gevent.spawn(producer_loop, stop, stats),
        gevent.spawn(churn_loop, stop, stats),
    ]
    for _ in range(NUM_WORKERS):
        greenlets.append(
            gevent.spawn(worker_loop, stop, stats, renewer_observed))

    start = time.time()
    while time.time() - start < DURATION_SECONDS:
        gevent.sleep(5)
        elapsed = int(time.time() - start)
        print(
            f"  t={elapsed:>3}s produced={stats.produced} "
            f"acked={stats.acked} lock_lost={stats.lock_lost} "
            f"reg_attr_err={stats.regression_attr_errors} "
            f"sdk_attr_err={stats.sdk_attr_errors} "
            f"other={dict(stats.other_errors)}"
        )

    stop.set()
    gevent.joinall(greenlets, timeout=30)
    gevent.sleep(3)
    settled_threads = threading.active_count()

    print()
    print("=== final ===")
    print(
        f"produced={stats.produced} acked={stats.acked} "
        f"lock_lost={stats.lock_lost}"
    )
    print(
        f"regression_attr_errors={stats.regression_attr_errors} "
        "(8878 signature)"
    )
    print(f"sdk_attr_errors={stats.sdk_attr_errors} (unrelated SDK socket)")
    print(f"other_errors={dict(stats.other_errors)}")
    print(
        f"distinct_renewers_observed={len({id(r) for r in renewer_observed})} "
        f"of {NUM_WORKERS} workers"
    )
    print(
        f"threads baseline={baseline_threads} settled={settled_threads}"
    )

    failures: list[str] = []
    if stats.produced == 0:
        failures.append("no messages produced")
    if stats.acked == 0:
        failures.append("no messages acked")
    if stats.regression_attr_errors:
        failures.append(
            f"regression_attr_errors={stats.regression_attr_errors}")
    handled = stats.acked + stats.lock_lost
    if handled >= 10 and (stats.lock_lost / handled) > 0.20:
        failures.append(
            f"lock_loss_rate={stats.lock_lost}/{handled} > 20%")
    if len({id(r) for r in renewer_observed}) != NUM_WORKERS:
        failures.append(
            f"expected {NUM_WORKERS} distinct renewers, "
            f"got {len({id(r) for r in renewer_observed})}")
    if (settled_threads - baseline_threads) > 4:
        failures.append(
            f"thread leak: settled={settled_threads} "
            f"baseline={baseline_threads}")

    if failures:
        print("FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("OK: all assertions held under gevent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
