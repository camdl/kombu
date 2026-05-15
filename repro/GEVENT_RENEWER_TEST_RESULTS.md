# Gevent + `use_lock_renewal` test results

PR #2545 ("Fix gevent workers not working with automatic lock renewal for azure service bus channel") proposes a custom `GeventLockRenewer`. The author claims `use_lock_renewal=True` from the just-merged #2542 doesn't work under gevent. These tests check that claim against merged main (`817ebae4`).

All runs use the local Azure Service Bus emulator (`repro/docker-compose.yml`) plus, for the uAMQP scenario, stunnel fronting it on port 5671. Renewal RPCs are counted by patching `ServiceBusReceiver.renew_message_lock` at class level after gevent monkey-patching.

Environment: Python 3.11, `azure-servicebus 7.14.3` (default pyamqp transport), `gevent 26.4.0`, `uamqp 1.6.11`, `celery 5.6.3`.

## Summary

| Test | Scope | Renewer test | Result |
|---|---|---|---|
| `chaos_azureservicebus_gevent.py` | original baseline, no instrumentation | indirect | PASS (0 lock_lost) |
| `chaos_azureservicebus_gevent_renewal.py` | 7 scenarios, instrumented | direct | 7/7 PASS |
| `chaos_azureservicebus_gevent_late_patch.py` | import before `patch_all()` | direct | PASS |
| `chaos_azureservicebus_gevent_uamqp.py` | uAMQP C extension via stunnel | direct | PASS |
| `celery_gevent_producer.py` (+ worker) | real celery, gevent pool, 5 tasks | end-to-end | PASS |
| `celery_gevent_extreme_producer.py` (+ worker) | 300 tasks, 50 greenlets, durations to 90s | end-to-end | PASS |

## 1. `chaos_azureservicebus_gevent_renewal.py`

Seven discrete scenarios with strict assertions, all instrumented at the SDK class-level so renewals are actually counted (not just inferred). `peek_lock_seconds=10` throughout unless noted.

| # | Scenario | Result |
|---|---|---|
| 1 | Short handler (no renewal needed) | 3 ack, 0 renewals fired |
| 2 | 25s handler, single greenlet | ack with 7 renewals on the message |
| 3 | 6 concurrent 25s handlers (one receive loop, dispatched) | 6/6 ack, ~6-7 renewals per message |
| 4 | 40s handler with `max_renewal_duration=15s` | lock_lost as expected; renewer correctly stopped after 4 renewals |
| 5 | Event-loop responsiveness (8 concurrent + heartbeat) | 8/8 ack; heartbeat p99 105ms, max 105ms |
| 6 | 2 Connections, separate renewers | 2/2 ack; 2 distinct renewer instances |
| 7 | 45s sustained mixed-duration stress | 18 ack, 0 lock_lost |

Thread count: baseline=1, settled=1 (no leak).

## 2. `chaos_azureservicebus_gevent_late_patch.py`

Anti-pattern check: imports `kombu` and `azure.servicebus` before `gevent.monkey.patch_all()`. If the SDK captured unpatched primitives at import time (a known gevent pitfall), the renewer would silently stop working.

- outcome: ack
- renewals_on_msg: 7
- total_renewals: 7

Late monkey-patching did not break the renewer. The `MonkeyPatchWarning` about pre-patched `ssl` is cosmetic.

## 3. `chaos_azureservicebus_gevent_uamqp.py`

Forces `uamqp_transport=True` for every `ServiceBusClient` and routes through stunnel (`repro/tls/stunnel.conf`) for TLS termination on 5671 to plaintext 5672. This is the combination PR #1788 was likely written for: the C extension that bypasses Python sockets.

- outcome: ack
- renewals_on_msg: 7
- total_renewals: 7
- heartbeat n=256, avg=100ms, p99=102ms, max=103ms

uAMQP under gevent did not block the event loop. The premise of PR #1788 (uAMQP+gevent broken) is not reproducible against `azure-servicebus 7.14.3`. SDK printed: *"uAMQP legacy support will be removed in the 7.15.0 minor release."*

## 4. `celery_gevent_producer.py` (+ `celery_gevent_worker.py`)

Real celery worker (`-P gevent -c 4`) consuming real tasks from `task.delay()`. peek_lock=10s, task durations 3/8/15/25/30s.

| Task | Duration | Starts | Ends |
|---|---|---|---|
| celery-gevent-0 | 3s | 1 | 1 |
| celery-gevent-1 | 8s | 1 | 1 |
| celery-gevent-2 | 15s | 1 | 1 |
| celery-gevent-3 | 25s | 1 | 1 |
| celery-gevent-4 | 30s | 1 | 1 |

All 5 tasks executed exactly once. Any renewer failure on the 15/25/30s tasks would have shown a second `start` event from broker redelivery.

## 5. `celery_gevent_extreme_producer.py` (+ extreme worker)

Maximum stress in the emulator's working envelope.

- 300 tasks (270 normal + 30 deliberately failing then retried)
- 50 concurrent gevent greenlets in the worker
- peek_lock=10s, max_lock_renewal_duration=600s
- Durations weighted across 1s to 90s (longest = 9× peek_lock)
- Throughput: 213s for 330 total executions
- 1,899 renewal RPCs fired during the run
- Worker heartbeat under load: avg 101ms, max 148ms

Per-duration outcome (`ends=N` per task):

| Duration | Tasks | Result |
|---|---|---|
| 1s | 30 | all `ends=1` |
| 3s | 41 | all `ends=1` |
| 8s | 43 | all `ends=1` |
| 15s | 60 | all `ends=1` |
| 25s | 32 | all `ends=1` |
| 40s | 24 | all `ends=1` |
| 60s | 19 | all `ends=1` |
| 90s | 21 | all `ends=1` |
| failing (retry) | 30 | all retried and `ends=1` |

Zero over-execution. Zero `MessageLockLostError`. The renewer kept locks alive across handlers up to 9× peek_lock under 50-greenlet concurrency for ~3.5 minutes of sustained pressure.

## What this means

`use_lock_renewal=True` on merged main works under gevent in every configuration tested: default pyamqp, late monkey-patching, uAMQP via TLS, real celery worker with the gevent pool, and an extreme 300-task stress. The renewer's RPCs fire at the expected cadence, locks stay alive, the event loop stays responsive (p99 ~100ms), and there is no over-delivery from lock loss.

The claim in PR #2545 ("this still doesn't work for gevent workers") is not reproducible from any setup we could construct. The most likely explanation: the symptoms originally seen were caused by the state-scoping bugs that #2543 and #2542 fixed (class-level `_queue_cache`, mode-collision in the receiver cache, `basic_cancel` wiping no-ack state), not by `AutoLockRenewer` itself. Under gevent those bugs would surface as misattributed "renewer doesn't work" behaviour because the high greenlet concurrency exposed the races more often.

## How to reproduce

```bash
docker compose -f repro/docker-compose.yml up -d
# For the uAMQP scenario, also:
stunnel /workspaces/kombu/repro/tls/stunnel.conf &

# Standalone chaos:
python repro/chaos_azureservicebus_gevent_renewal.py
python repro/chaos_azureservicebus_gevent_late_patch.py
python repro/chaos_azureservicebus_gevent_uamqp.py

# Celery end-to-end (worker in one shell, producer in another):
python repro/celery_gevent_worker.py
python repro/celery_gevent_producer.py

# Extreme celery end-to-end:
python repro/celery_gevent_extreme_worker.py
python repro/celery_gevent_extreme_producer.py
```
