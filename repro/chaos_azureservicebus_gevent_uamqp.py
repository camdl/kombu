"""uAMQP-forced gevent renewer scenario, via stunnel TLS.

The legacy combination PR #1788 was likely written for: uAMQP (C
extension) under gevent. The C transport bypasses Python sockets, so
gevent.monkey.patch_all() can't make AMQP I/O cooperative. If
AutoLockRenewer's renewal RPCs hang the event loop or fail to renew
under uAMQP, that would be evidence the gevent renewer in PR #2545
addresses a real problem (just not one kombu's default transport
exposes).

Wiring: the emulator listens on plaintext 5672; stunnel terminates
TLS on 5671 in front of it. uAMQP requires TLS, so it connects via
5671 with the stunnel cert as CA.

Prereqs:
  pip show uamqp     (should be >=1.6.3,<2.0.0)
  stunnel /workspaces/kombu/repro/tls/stunnel.conf &   # backgrounded

Run:
  docker compose -f repro/docker-compose.yml up -d
  stunnel repro/tls/stunnel.conf &
  python repro/chaos_azureservicebus_gevent_uamqp.py

Result (run 2026-05-15, against merged main 817ebae4):
  PASS. outcome=ack, renewals_on_msg=7, total_renewals=7.
  Heartbeat n=256, avg=100ms, p99=102ms, max=103ms. The C extension
  uAMQP transport did NOT block the event loop under gevent. The
  premise of PR #1788 (uAMQP + gevent broken) is not reproducible
  against current azure-servicebus 7.14.3.
  SDK printed DeprecationWarning ("uAMQP legacy support will be
  removed in 7.15.0 minor release").
"""
from __future__ import annotations

from gevent import monkey
monkey.patch_all()  # noqa: E402

import time  # noqa: E402
from collections import defaultdict  # noqa: E402

import gevent  # noqa: E402
from gevent.event import Event as GEvent  # noqa: E402

try:
    import uamqp  # noqa: F401
except ImportError:
    print("uamqp not installed; skipping")
    raise SystemExit(0)

import azure.servicebus  # noqa: E402
import azure.servicebus.exceptions  # noqa: E402
from azure.servicebus import (  # noqa: E402
    ServiceBusClient, ServiceBusReceiver, ServiceBusReceiveMode)

CA = "/workspaces/kombu/repro/tls/cert.pem"

# Force every ServiceBusClient to use uAMQP + the stunnel CA.
_orig_from_conn = ServiceBusClient.from_connection_string.__func__
_orig_init = ServiceBusClient.__init__


def _from_conn(cls, conn_str, **kwargs):
    kwargs.setdefault("uamqp_transport", True)
    kwargs.setdefault("connection_verify", CA)
    return _orig_from_conn(cls, conn_str, **kwargs)


def _init(self, fully_qualified_namespace, credential, **kwargs):
    kwargs.setdefault("uamqp_transport", True)
    kwargs.setdefault("connection_verify", CA)
    return _orig_init(
        self, fully_qualified_namespace, credential, **kwargs)


ServiceBusClient.from_connection_string = classmethod(_from_conn)
ServiceBusClient.__init__ = _init


from kombu import Connection  # noqa: E402

# Use the TLS port. UseDevelopmentEmulator=true must NOT be present
# (it forces use_tls=False on pyamqp; uamqp ignores it).
URL = ("azureservicebus://RootManageSharedAccessKey:SAS_KEY_VALUE@"
       "localhost:5671")
EMU = ("Endpoint=sb://localhost:5671;"
       "SharedAccessKeyName=RootManageSharedAccessKey;"
       "SharedAccessKey=SAS_KEY_VALUE;")
QUEUE = "test-renewal"

renewal_calls: dict[int, list[float]] = defaultdict(list)
renewal_total = [0]
_orig_renew = ServiceBusReceiver.renew_message_lock


def counted(self, message, **kwargs):
    renewal_total[0] += 1
    renewal_calls[id(message)].append(time.monotonic())
    return _orig_renew(self, message, **kwargs)


ServiceBusReceiver.renew_message_lock = counted


def _attach_emulator(channel) -> None:
    channel._connection_string = EMU
    channel._namespace = "localhost:5671"


def make_conn(*, use_renewal=True, peek_lock=10, max_renewal=3600):
    opts = {
        "peek_lock_seconds": peek_lock,
        "wait_time_seconds": 1,
    }
    if use_renewal:
        opts["use_lock_renewal"] = True
        opts["max_lock_renewal_duration"] = max_renewal
    return Connection(URL, transport_options=opts)


def drain_queue() -> int:
    conn = make_conn(use_renewal=False)
    n = 0
    try:
        ch = conn.channel()
        _attach_emulator(ch)
        with ch.queue_service.get_queue_receiver(
            queue_name=ch.entity_name(QUEUE),
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        ) as r:
            while True:
                msgs = r.receive_messages(
                    max_message_count=10, max_wait_time=0.5)
                if not msgs:
                    break
                n += len(msgs)
    finally:
        conn.release()
    return n


def publish_one(prefix: str = "uamqp") -> None:
    conn = make_conn(use_renewal=False)
    try:
        ch = conn.channel()
        _attach_emulator(ch)
        ch._put(QUEUE, {"body": prefix, "properties": {}})
    finally:
        conn.release()


def main() -> int:
    print(f"gevent threading patched: "
          f"{monkey.is_module_patched('threading')}")
    print(f"gevent socket patched: "
          f"{monkey.is_module_patched('socket')}")
    print("Forcing uamqp_transport=True; TLS via stunnel on :5671")

    drained = drain_queue()
    print(f"drained residual={drained}")
    publish_one("uamqp-single")

    # Heartbeat to detect event loop blocking.
    heartbeat_deltas: list[float] = []
    stop = GEvent()

    def heartbeat() -> None:
        last = time.monotonic()
        while not stop.is_set():
            gevent.sleep(0.1)
            now = time.monotonic()
            heartbeat_deltas.append(now - last)
            last = now

    msg_id = None
    outcome = "none"

    conn = make_conn(use_renewal=True, peek_lock=10, max_renewal=60)
    try:
        hb = gevent.spawn(heartbeat)
        ch = conn.channel()
        _attach_emulator(ch)
        recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver
        msgs = recv.receive_messages(max_message_count=1, max_wait_time=10)
        if not msgs:
            stop.set()
            hb.join()
            print("FAIL: no message received under uamqp")
            return 1
        msg = msgs[0]
        msg_id = id(msg)
        print("Holding 25s under uAMQP + gevent (peek_lock=10s)...")
        gevent.sleep(25)
        try:
            recv.complete_message(msg)
            outcome = "ack"
        except azure.servicebus.exceptions.MessageLockLostError:
            outcome = "lock_lost"
        stop.set()
        hb.join(timeout=2)
    finally:
        conn.release()

    per_msg = len(renewal_calls.get(msg_id, [])) if msg_id else 0
    if heartbeat_deltas:
        max_gap = max(heartbeat_deltas)
        p99 = sorted(heartbeat_deltas)[
            max(0, int(len(heartbeat_deltas) * 0.99) - 1)]
        avg = sum(heartbeat_deltas) / len(heartbeat_deltas)
    else:
        max_gap = p99 = avg = 0.0

    print(f"outcome={outcome} renewals_on_msg={per_msg} "
          f"total_renewals={renewal_total[0]}")
    print(f"heartbeat n={len(heartbeat_deltas)} avg={avg*1000:.0f}ms "
          f"p99={p99*1000:.0f}ms max={max_gap*1000:.0f}ms")

    failures = []
    if outcome != "ack":
        failures.append(f"outcome={outcome} (expected ack)")
    if per_msg < 2:
        failures.append(
            f"only {per_msg} renewals over 25s with peek_lock=10s")
    if max_gap > 1.0:
        failures.append(
            f"event loop blocked >1s (max gap {max_gap*1000:.0f}ms)")
    if p99 > 0.5:
        failures.append(
            f"event loop frequently blocked (p99 {p99*1000:.0f}ms)")

    if failures:
        print(f"FAIL: {'; '.join(failures)}")
        print("  (legacy uAMQP + gevent combo has a real issue)")
        return 1
    print("PASS: uAMQP + gevent renews locks and stays responsive")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
