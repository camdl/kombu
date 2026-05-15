"""Antipattern: import kombu and azure.servicebus BEFORE monkey-patch.

If users do `from kombu import Connection` before
`gevent.monkey.patch_all()`, the SDK captures unpatched references
(threading.Event, time.sleep, ThreadPoolExecutor). Question: does
AutoLockRenewer still work, or does it silently fail because it's
holding pre-patch primitives?

This is a known gevent pitfall and a plausible explanation for
"AutoLockRenewer doesn't work under gevent" reports.

Run after starting the emulator:
  docker compose -f repro/docker-compose.yml up -d
  python repro/chaos_azureservicebus_gevent_late_patch.py

Result (run 2026-05-15, against merged main 817ebae4):
  PASS. outcome=ack, renewals_on_msg=7, total_renewals=7.
  Late monkey-patching did not break the renewer. The MonkeyPatchWarning
  about pre-patched ssl is cosmetic and unrelated.
"""
from __future__ import annotations

import azure.servicebus  # noqa: E402, F401  (intentionally pre-patch)
import azure.servicebus.exceptions  # noqa: E402, F401
from azure.servicebus import ServiceBusReceiver  # noqa: E402
from kombu import Connection  # noqa: E402, F401

import time  # noqa: E402
from collections import defaultdict  # noqa: E402

from gevent import monkey  # noqa: E402
monkey.patch_all()  # late patch

import gevent  # noqa: E402

URL = "azureservicebus://RootManageSharedAccessKey:SAS_KEY_VALUE@localhost.net"
EMU = ("Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
       "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")
QUEUE = "test-renewal"

renewal_calls: dict[int, list[float]] = defaultdict(list)
renewal_total = [0]
_orig = ServiceBusReceiver.renew_message_lock


def counted(self, message, **kwargs):
    renewal_total[0] += 1
    renewal_calls[id(message)].append(time.monotonic())
    return _orig(self, message, **kwargs)


ServiceBusReceiver.renew_message_lock = counted


def _attach_emulator(ch):
    ch._connection_string = EMU
    ch._namespace = "localhost"


def main() -> int:
    print(f"threading patched: {monkey.is_module_patched('threading')}")
    print(f"socket patched: {monkey.is_module_patched('socket')}")
    print("kombu and azure.servicebus were imported BEFORE patch_all()")

    conn = Connection(URL, transport_options={
        "peek_lock_seconds": 10,
        "wait_time_seconds": 1,
        "use_lock_renewal": True,
        "max_lock_renewal_duration": 60,
    })
    try:
        ch = conn.channel()
        _attach_emulator(ch)

        # Drain residual.
        from azure.servicebus import ServiceBusReceiveMode
        with ch.queue_service.get_queue_receiver(
            queue_name=ch.entity_name(QUEUE),
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        ) as r:
            while r.receive_messages(max_message_count=10, max_wait_time=0.5):
                pass

        # Publish one message via separate non-renewal connection.
        pub = Connection(URL, transport_options={
            "peek_lock_seconds": 10, "wait_time_seconds": 1})
        try:
            pch = pub.channel()
            _attach_emulator(pch)
            pch._put(QUEUE, {"body": "late-patch", "properties": {}})
        finally:
            pub.release()

        recv = ch._get_asb_receiver(ch.entity_name(QUEUE)).receiver
        msgs = recv.receive_messages(max_message_count=1, max_wait_time=10)
        if not msgs:
            print("FAIL: no message received")
            return 1
        msg = msgs[0]
        msg_id = id(msg)
        print("Holding message for 25s under late-patched gevent...")
        gevent.sleep(25)
        try:
            recv.complete_message(msg)
            outcome = "ack"
        except azure.servicebus.exceptions.MessageLockLostError:
            outcome = "lock_lost"
    finally:
        conn.release()

    per_msg = len(renewal_calls.get(msg_id, []))
    print(f"outcome={outcome} renewals_on_msg={per_msg} "
          f"total_renewals={renewal_total[0]}")
    if outcome == "ack" and per_msg >= 2:
        print("PASS: late-patching did not break the renewer")
        return 0
    print("FAIL: renewer didn't work under late-patching")
    print("  (this would be a real gevent pitfall to document)")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
