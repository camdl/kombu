"""Exhaustive gevent renewal chaos.

The existing chaos script (`chaos_azureservicebus_gevent.py`) proved
the renewer is *created and torn down* correctly under gevent. It
did not prove the renewer actually does its job (renewing locks
mid-handler). This script does.

Mechanism: patch `ServiceBusReceiver.renew_message_lock` at the class
level (after gevent monkey-patching) to count calls and record
timestamps. Then run discrete scenarios with deterministic handler
durations and strict assertions.

Scenarios:
  1. Short handler (no renewal needed) - sanity.
  2. Long handler, single greenlet, handler > peek_lock.
  3. Long handler, N concurrent greenlets.
  4. Handler exceeds max_lock_renewal_duration (renewer should stop;
     lock loss is expected and asserted).
  5. Event-loop responsiveness during concurrent renewals.
  6. Multiple Connections, each with its own renewer.
  7. Sustained mixed-duration stress with strict lock-loss tolerance.

Run:
  docker compose -f repro/docker-compose.yml up -d
  python repro/chaos_azureservicebus_gevent_renewal.py

Results (run 2026-05-15, against merged main 817ebae4):
  All 7 scenarios PASS.
    1. short handler: 3/3 ack, 0 renewals (none needed)
    2. 25s handler / peek_lock=10s: ack with 7 renewals on the message
    3. 6 concurrent 25s handlers: 6/6 ack, 6-7 renewals each
    4. handler 40s / max_renewal=15s: lock_lost as expected after
       4 renewals (renewer stopped at max_duration)
    5. 8 concurrent renewers, 0.1s heartbeat: p99 105ms max 105ms
       (event loop never blocks)
    6. 2 Connections, distinct renewers: 2/2 ack, 2 distinct instances
    7. 45s sustained stress, mixed handlers: 18 ack, 0 lock_lost
  Thread count baseline=1 settled=1 (no leak).
"""
from __future__ import annotations

from gevent import monkey
monkey.patch_all()  # noqa: E402

import time  # noqa: E402
import threading  # noqa: E402
from collections import defaultdict  # noqa: E402

import gevent  # noqa: E402
from gevent.event import Event as GEvent  # noqa: E402

import azure.servicebus.exceptions  # noqa: E402
from azure.servicebus import (  # noqa: E402
    ServiceBusReceiver, ServiceBusReceiveMode)
from kombu import Connection  # noqa: E402

URL = "azureservicebus://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost.net"
EMU = ("Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
       "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
QUEUE = "test-renewal"

renewal_calls: dict[int, list[float]] = defaultdict(list)
renewal_total = [0]
_orig_renew = ServiceBusReceiver.renew_message_lock


def counted_renew(self, message, **kwargs):
    renewal_total[0] += 1
    renewal_calls[id(message)].append(time.monotonic())
    return _orig_renew(self, message, **kwargs)


ServiceBusReceiver.renew_message_lock = counted_renew


def _attach_emulator(channel) -> None:
    channel._connection_string = EMU
    channel._namespace = "localhost"


def make_conn(*, use_renewal=True, peek_lock=10, max_renewal=3600):
    opts = {
        "peek_lock_seconds": peek_lock,
        "wait_time_seconds": 1,
    }
    if use_renewal:
        opts["use_lock_renewal"] = True
        opts["max_lock_renewal_duration"] = max_renewal
    return Connection(URL, transport_options=opts)


def publish_n(n: int, prefix: str = "msg") -> None:
    conn = make_conn(use_renewal=False)
    try:
        ch = conn.channel()
        _attach_emulator(ch)
        for i in range(n):
            ch._put(QUEUE, {"body": f"{prefix}-{i}", "properties": {}})
    finally:
        conn.release()


def drain_queue() -> int:
    conn = make_conn(use_renewal=False)
    n = 0
    try:
        ch = conn.channel()
        _attach_emulator(ch)
        with ch.queue_service.get_queue_receiver(
            queue_name=ch.entity_name(QUEUE),
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        ) as receiver:
            while True:
                msgs = receiver.receive_messages(
                    max_message_count=10, max_wait_time=0.5)
                if not msgs:
                    break
                n += len(msgs)
    finally:
        conn.release()
    return n


def reset_counters() -> None:
    renewal_calls.clear()
    renewal_total[0] = 0


def banner(label: str) -> None:
    print()
    print("=" * 70)
    print(label)
    print("=" * 70)


def scenario_1_short_handler() -> tuple[bool, str]:
    banner("Scenario 1: short handler (no renewal needed)")
    drained = drain_queue()
    publish_n(3, "s1")
    print(f"  drained_residual={drained} published=3")
    reset_counters()

    conn = make_conn(use_renewal=True, peek_lock=10)
    try:
        ch = conn.channel()
        _attach_emulator(ch)
        recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver
        acked = lock_lost = 0
        for _ in range(3):
            msgs = recv.receive_messages(max_message_count=1, max_wait_time=5)
            if not msgs:
                break
            msg = msgs[0]
            gevent.sleep(2)
            try:
                recv.complete_message(msg)
                acked += 1
            except azure.servicebus.exceptions.MessageLockLostError:
                lock_lost += 1
    finally:
        conn.release()

    print(f"  acked={acked} lock_lost={lock_lost} "
          f"renewals_fired={renewal_total[0]}")
    if acked == 3 and lock_lost == 0:
        return True, "short handler completed cleanly"
    return False, f"acked={acked} lock_lost={lock_lost} expected 3/0"


def scenario_2_long_handler_single() -> tuple[bool, str]:
    banner("Scenario 2: long handler (25s), single greenlet, "
           "peek_lock=10s")
    drained = drain_queue()
    publish_n(1, "s2")
    print(f"  drained_residual={drained} published=1")
    reset_counters()

    conn = make_conn(use_renewal=True, peek_lock=10)
    try:
        ch = conn.channel()
        _attach_emulator(ch)
        recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver
        msgs = recv.receive_messages(max_message_count=1, max_wait_time=10)
        if not msgs:
            return False, "no message received"
        msg = msgs[0]
        msg_id = id(msg)
        print(f"  message received, sleeping 25s with renewal active...")
        gevent.sleep(25)
        try:
            recv.complete_message(msg)
            outcome = "ack"
        except azure.servicebus.exceptions.MessageLockLostError:
            outcome = "lock_lost"
    finally:
        conn.release()

    per_msg_renewals = len(renewal_calls.get(msg_id, []))
    print(f"  outcome={outcome} renewals_fired={renewal_total[0]} "
          f"on_target_msg={per_msg_renewals}")
    if outcome != "ack":
        return False, f"outcome={outcome} (expected ack)"
    if per_msg_renewals < 2:
        return False, (f"only {per_msg_renewals} renewals over 25s with "
                       f"peek_lock=10s (renewer not actually working)")
    return True, f"completed after {per_msg_renewals} renewals"


def scenario_3_long_handler_concurrent() -> tuple[bool, str]:
    n_workers = 6
    banner(f"Scenario 3: long handler (25s) × {n_workers} concurrent "
           f"handlers (celery-style: one receive loop, dispatched)")
    drained = drain_queue()
    publish_n(n_workers, "s3")
    print(f"  drained_residual={drained} published={n_workers}")
    reset_counters()

    # Realistic shape: one Connection, one receiver, N concurrent
    # handler greenlets. Mirrors how celery dispatches tasks under
    # gevent, not N greenlets all calling receive_messages.
    conn = make_conn(use_renewal=True, peek_lock=10)
    results = {"acked": 0, "lock_lost": 0, "msg_ids": []}

    try:
        ch = conn.channel()
        _attach_emulator(ch)
        recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver

        all_msgs: list = []
        deadline = time.monotonic() + 15
        while (len(all_msgs) < n_workers
                and time.monotonic() < deadline):
            msgs = recv.receive_messages(
                max_message_count=n_workers, max_wait_time=2)
            all_msgs.extend(msgs)

        if len(all_msgs) < n_workers:
            return False, (f"only received {len(all_msgs)} of "
                           f"{n_workers} expected messages")
        for m in all_msgs:
            results["msg_ids"].append(id(m))

        def handle(msg) -> None:
            gevent.sleep(25)
            try:
                recv.complete_message(msg)
                results["acked"] += 1
            except azure.servicebus.exceptions.MessageLockLostError:
                results["lock_lost"] += 1

        gevent.joinall(
            [gevent.spawn(handle, m) for m in all_msgs], timeout=60)
    finally:
        conn.release()

    per_msg = [len(renewal_calls.get(m, [])) for m in results["msg_ids"]]
    print(f"  acked={results['acked']} lock_lost={results['lock_lost']} "
          f"total_renewals={renewal_total[0]} per_msg={per_msg}")
    if results["acked"] != n_workers:
        return False, (f"acked={results['acked']} expected {n_workers}; "
                       f"lock_lost={results['lock_lost']}")
    if any(c < 2 for c in per_msg):
        return False, f"some messages had <2 renewals: {per_msg}"
    return True, (f"all {n_workers} completed; "
                  f"renewals per message {per_msg}")


def scenario_4_exceeds_max_renewal() -> tuple[bool, str]:
    banner("Scenario 4: handler exceeds max_lock_renewal_duration "
           "(renewer should stop)")
    drained = drain_queue()
    publish_n(1, "s4")
    print(f"  drained_residual={drained} published=1")
    reset_counters()

    # max_renewal=15s, handler=40s, peek_lock=10s.
    # Renewer should keep lock for ~15s, then stop. Lock expires ~10s
    # later, so by t=25s the lock is gone. Complete at t=40s should
    # raise MessageLockLostError.
    conn = make_conn(use_renewal=True, peek_lock=10, max_renewal=15)
    try:
        ch = conn.channel()
        _attach_emulator(ch)
        recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver
        msgs = recv.receive_messages(max_message_count=1, max_wait_time=10)
        if not msgs:
            return False, "no message received"
        msg = msgs[0]
        msg_id = id(msg)
        print(f"  message received, sleeping 40s "
              f"(max_renewal=15s)...")
        gevent.sleep(40)
        try:
            recv.complete_message(msg)
            outcome = "ack"
        except azure.servicebus.exceptions.MessageLockLostError:
            outcome = "lock_lost"
    finally:
        conn.release()

    per_msg = len(renewal_calls.get(msg_id, []))
    print(f"  outcome={outcome} renewals_fired_on_target={per_msg}")
    if outcome != "lock_lost":
        return False, (f"outcome={outcome} expected lock_lost; "
                       f"renewer ignored max_lock_renewal_duration?")
    # Expect roughly 2-3 renewals within the 15s window.
    if per_msg < 1:
        return False, f"renewer never fired ({per_msg} renewals)"
    return True, (f"renewer correctly stopped after max_duration; "
                  f"{per_msg} renewals before expiry")


def scenario_5_event_loop_responsive() -> tuple[bool, str]:
    n_workers = 8
    banner(f"Scenario 5: event-loop responsiveness "
           f"({n_workers} concurrent renewing handlers)")
    drained = drain_queue()
    publish_n(n_workers, "s5")
    print(f"  drained_residual={drained} published={n_workers}")
    reset_counters()

    heartbeat_deltas: list[float] = []
    stop = GEvent()

    def heartbeat() -> None:
        last = time.monotonic()
        while not stop.is_set():
            gevent.sleep(0.1)
            now = time.monotonic()
            heartbeat_deltas.append(now - last)
            last = now

    conn = make_conn(use_renewal=True, peek_lock=10)
    acked = [0]
    lock_lost = [0]

    try:
        hb = gevent.spawn(heartbeat)
        ch = conn.channel()
        _attach_emulator(ch)
        recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver

        all_msgs: list = []
        deadline = time.monotonic() + 15
        while (len(all_msgs) < n_workers
                and time.monotonic() < deadline):
            msgs = recv.receive_messages(
                max_message_count=n_workers, max_wait_time=2)
            all_msgs.extend(msgs)

        def handle(msg) -> None:
            gevent.sleep(20)
            try:
                recv.complete_message(msg)
                acked[0] += 1
            except azure.servicebus.exceptions.MessageLockLostError:
                lock_lost[0] += 1

        gevent.joinall(
            [gevent.spawn(handle, m) for m in all_msgs], timeout=45)
        stop.set()
        hb.join(timeout=5)
    finally:
        conn.release()

    samples = heartbeat_deltas
    if not samples:
        return False, "no heartbeat samples"
    max_gap = max(samples)
    p99 = sorted(samples)[max(0, int(len(samples) * 0.99) - 1)]
    avg = sum(samples) / len(samples)
    print(f"  acked={acked[0]} lock_lost={lock_lost[0]} "
          f"heartbeat n={len(samples)} avg={avg*1000:.0f}ms "
          f"p99={p99*1000:.0f}ms max={max_gap*1000:.0f}ms")
    if acked[0] != n_workers:
        return False, (f"acked={acked[0]} expected {n_workers}; "
                       f"lock_lost={lock_lost[0]}")
    if max_gap > 1.0:
        return False, (f"event loop blocked at least once "
                       f"(max heartbeat gap {max_gap*1000:.0f}ms)")
    if p99 > 0.5:
        return False, (f"event loop frequently blocked "
                       f"(p99 heartbeat gap {p99*1000:.0f}ms)")
    return True, (f"event loop stayed responsive; "
                  f"p99 gap {p99*1000:.0f}ms max {max_gap*1000:.0f}ms")


def scenario_6_multiple_connections() -> tuple[bool, str]:
    n_conns = 2  # emulator connection quota is tight; 2 is enough to prove
    banner(f"Scenario 6: {n_conns} Connections, each with its own renewer")
    drained = drain_queue()
    publish_n(n_conns, "s6")
    print(f"  drained_residual={drained} published={n_conns}")
    reset_counters()

    conns = [make_conn(use_renewal=True, peek_lock=10)
             for _ in range(n_conns)]
    renewers_seen: list[object] = []
    acked = [0]
    lock_lost = [0]

    def worker(conn: Connection) -> None:
        try:
            ch = conn.channel()
            _attach_emulator(ch)
            recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver
            renewers_seen.append(conn.transport._renewer)
            msgs = recv.receive_messages(
                max_message_count=1, max_wait_time=10)
            if not msgs:
                return
            msg = msgs[0]
            gevent.sleep(20)
            try:
                recv.complete_message(msg)
                acked[0] += 1
            except azure.servicebus.exceptions.MessageLockLostError:
                lock_lost[0] += 1
        except Exception as exc:
            print(f"  worker error: {exc!r}")

    try:
        gevent.joinall(
            [gevent.spawn(worker, c) for c in conns], timeout=45)
    finally:
        for c in conns:
            c.release()
        gevent.sleep(1)

    distinct = len({id(r) for r in renewers_seen if r is not None})
    print(f"  acked={acked[0]} lock_lost={lock_lost[0]} "
          f"distinct_renewers={distinct}/{n_conns}")
    if acked[0] != n_conns:
        return False, (f"acked={acked[0]} expected {n_conns}; "
                       f"lock_lost={lock_lost[0]}")
    if distinct != n_conns:
        return False, (f"expected {n_conns} distinct renewers, "
                       f"got {distinct}")
    return True, f"all {n_conns} connections completed independently"


def scenario_7_sustained_stress() -> tuple[bool, str]:
    n_workers = 6
    duration = 45
    banner(f"Scenario 7: sustained mixed-duration stress "
           f"({n_workers} workers × {duration}s)")
    drained = drain_queue()
    print(f"  drained_residual={drained}")
    reset_counters()

    stop = GEvent()
    stats: dict[str, int] = {"produced": 0, "acked": 0, "lock_lost": 0,
                              "errors": 0}

    def producer() -> None:
        conn = make_conn(use_renewal=False)
        try:
            ch = conn.channel()
            _attach_emulator(ch)
            i = 0
            while not stop.is_set():
                try:
                    ch._put(QUEUE,
                            {"body": f"s7-{i}", "properties": {}})
                    stats["produced"] += 1
                except Exception:
                    stats["errors"] += 1
                i += 1
                stop.wait(0.25)
        finally:
            conn.release()

    def worker(idx: int) -> None:
        conn = make_conn(use_renewal=True, peek_lock=10)
        try:
            ch = conn.channel()
            _attach_emulator(ch)
            recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver
            handler_durations = [3, 8, 15, 22]
            i = 0
            while not stop.is_set():
                try:
                    msgs = recv.receive_messages(
                        max_message_count=1, max_wait_time=2)
                except Exception:
                    stats["errors"] += 1
                    continue
                for msg in msgs:
                    dur = handler_durations[i % len(handler_durations)]
                    i += 1
                    deadline = time.monotonic() + dur
                    while (time.monotonic() < deadline
                            and not stop.is_set()):
                        gevent.sleep(0.5)
                    if stop.is_set():
                        break
                    try:
                        recv.complete_message(msg)
                        stats["acked"] += 1
                    except (azure.servicebus.exceptions
                            .MessageLockLostError):
                        stats["lock_lost"] += 1
                    except Exception:
                        stats["errors"] += 1
        finally:
            conn.release()

    greenlets = [gevent.spawn(producer)]
    greenlets += [gevent.spawn(worker, i) for i in range(n_workers)]

    start = time.monotonic()
    while time.monotonic() - start < duration:
        gevent.sleep(5)
        elapsed = int(time.monotonic() - start)
        print(f"  t={elapsed:>3}s produced={stats['produced']} "
              f"acked={stats['acked']} lock_lost={stats['lock_lost']} "
              f"errors={stats['errors']}")

    stop.set()
    gevent.joinall(greenlets, timeout=30)

    handled = stats["acked"] + stats["lock_lost"]
    rate = (stats["lock_lost"] / handled) if handled else 0.0
    print(f"  final: produced={stats['produced']} acked={stats['acked']} "
          f"lock_lost={stats['lock_lost']} errors={stats['errors']} "
          f"lock_loss_rate={rate*100:.1f}%")
    if handled < 5:
        return False, f"only {handled} messages handled; broker issue?"
    if rate > 0.02:
        return False, (f"lock_loss_rate {rate*100:.1f}% exceeds 2%; "
                       f"renewer not keeping up under stress")
    return True, (f"handled={handled} lock_loss_rate={rate*100:.1f}% "
                  f"(threshold 2%)")


def main() -> int:
    is_patched = monkey.is_module_patched("threading")
    is_socket_patched = monkey.is_module_patched("socket")
    print(f"gevent monkey-patched: threading={is_patched} "
          f"socket={is_socket_patched}")
    if not (is_patched and is_socket_patched):
        print("FAIL: monkey-patching incomplete; aborting")
        return 1

    baseline_threads = threading.active_count()
    print(f"baseline_threads={baseline_threads}")

    scenarios = [
        scenario_1_short_handler,
        scenario_2_long_handler_single,
        scenario_3_long_handler_concurrent,
        scenario_4_exceeds_max_renewal,
        scenario_5_event_loop_responsive,
        scenario_6_multiple_connections,
        scenario_7_sustained_stress,
    ]
    results = []
    for s in scenarios:
        try:
            ok, msg = s()
        except Exception as exc:
            import traceback
            traceback.print_exc()
            ok, msg = False, f"raised {type(exc).__name__}: {exc}"
        results.append((s.__name__, ok, msg))
        print(f"  {'PASS' if ok else 'FAIL'}: {msg}")
        # Let the emulator release AMQP connections between scenarios.
        gevent.sleep(3)

    gevent.sleep(3)
    settled_threads = threading.active_count()
    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    for name, ok, msg in results:
        print(f"  {'PASS' if ok else 'FAIL'} {name}: {msg}")
    print(f"threads baseline={baseline_threads} settled={settled_threads}")
    if settled_threads - baseline_threads > 4:
        print("WARN: possible thread leak")

    failed = [r for r in results if not r[1]]
    if failed:
        print(f"\n{len(failed)} scenario(s) FAILED")
        return 1
    print("\nAll scenarios PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
