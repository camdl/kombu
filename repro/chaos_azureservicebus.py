"""Concurrent battle test for the azureservicebus transport.

Realistic worker simulation against the local Azure Service Bus
emulator:

* One producer ``kombu.Connection`` publishes a steady stream to the
  ``test-renewal`` queue.
* Four "worker" ``kombu.Connection`` instances each open one Channel
  and one receiver with ``use_lock_renewal=True``,
  ``peek_lock_seconds=10``, ``max_lock_renewal_duration=120``. Each
  worker sleeps a random amount that can exceed the 10 second broker
  lock, so renewal is the only thing keeping the ack valid.
* A short-lived "churn" connection opens and releases in the
  background to exercise teardown overlapping with in-flight publishes.

The 8878 regression signature is a NoneType ``AttributeError`` on the
SDK's ``send_messages`` / ``create_sender_link`` path under the kombu
``_put`` -> ``queue_obj.sender.send_messages(...)`` frame. Other
NoneType errors from the SDK's internal socket handling under
concurrent receive (unrelated to the cache fix) are counted
separately and reported but do not fail the run.

Run after starting the emulator:

    docker compose -f repro/docker-compose.yml up -d
    python repro/chaos_azureservicebus.py

This file is not committed; it is local scaffolding for verifying the
auto-lock-renewer rebase.
"""
from __future__ import annotations

import random
import threading
import time
import traceback
from collections import Counter

import azure.servicebus.exceptions
from kombu import Connection

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
        self._lock = threading.Lock()

    def bump(self, key: str) -> None:
        with self._lock:
            setattr(self, key, getattr(self, key) + 1)

    def record_error(self, exc: BaseException) -> None:
        with self._lock:
            if isinstance(exc, AttributeError):
                tb = "".join(traceback.format_exception(
                    type(exc), exc, exc.__traceback__))
                if any(p in tb for p in EIGHT_EIGHT_SEVEN_EIGHT_PATTERNS):
                    self.regression_attr_errors += 1
                else:
                    self.sdk_attr_errors += 1
            self.other_errors[type(exc).__name__] += 1


def producer_loop(stop: threading.Event, stats: Stats) -> None:
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


def worker_loop(stop: threading.Event, stats: Stats,
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


def churn_loop(stop: threading.Event, stats: Stats) -> None:
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
    print(
        f"chaos run: duration={DURATION_SECONDS}s workers={NUM_WORKERS} "
        f"producer_rate={PRODUCER_RATE_PER_SEC}/s "
        f"handler_sleep={HANDLER_SLEEP_RANGE} "
        f"peek_lock={PEEK_LOCK_SECONDS}s renewal={MAX_RENEWAL_SECONDS}s"
    )
    stats = Stats()
    baseline_threads = threading.active_count()
    stop = threading.Event()
    renewer_observed: list[object] = []

    workers: list[threading.Thread] = []
    producer = threading.Thread(
        target=producer_loop, args=(stop, stats), daemon=True)
    producer.start()
    workers.append(producer)

    for _ in range(NUM_WORKERS):
        t = threading.Thread(
            target=worker_loop,
            args=(stop, stats, renewer_observed),
            daemon=True,
        )
        t.start()
        workers.append(t)

    churn = threading.Thread(
        target=churn_loop, args=(stop, stats), daemon=True)
    churn.start()
    workers.append(churn)

    start = time.time()
    while time.time() - start < DURATION_SECONDS:
        time.sleep(5)
        elapsed = int(time.time() - start)
        print(
            f"  t={elapsed:>3}s produced={stats.produced} "
            f"acked={stats.acked} lock_lost={stats.lock_lost} "
            f"reg_attr_err={stats.regression_attr_errors} "
            f"sdk_attr_err={stats.sdk_attr_errors} "
            f"other={dict(stats.other_errors)}"
        )

    stop.set()
    for t in workers:
        t.join(timeout=20)
    time.sleep(3)
    settled_threads = threading.active_count()

    print()
    print("=== final ===")
    print(
        f"produced={stats.produced} acked={stats.acked} "
        f"lock_lost={stats.lock_lost}"
    )
    print(
        f"regression_attr_errors={stats.regression_attr_errors} "
        f"(8878 signature: send_messages / create_sender_link on NoneType)"
    )
    print(
        f"sdk_attr_errors={stats.sdk_attr_errors} "
        f"(unrelated SDK socket NoneType under concurrent receive)"
    )
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
        failures.append("no messages produced (broker unreachable?)")
    if stats.acked == 0:
        failures.append("no messages acked (consumers never settled)")
    if stats.regression_attr_errors:
        failures.append(
            f"regression_attr_errors={stats.regression_attr_errors} "
            "(8878 signature observed)")
    handled = stats.acked + stats.lock_lost
    if handled >= 10 and (stats.lock_lost / handled) > 0.20:
        failures.append(
            f"lock_loss_rate={stats.lock_lost}/{handled} > 20% "
            "(renewer not keeping up with slow handlers)")
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
    print("OK: all assertions held")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
