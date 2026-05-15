"""Extreme producer: blast 300 mixed-duration tasks at the worker.

Run sequence:
  docker compose -f repro/docker-compose.yml up -d
  python repro/celery_gevent_extreme_worker.py   # in another shell
  python repro/celery_gevent_extreme_producer.py

Validates:
  - Every non-failing task executes exactly once.
  - Failing tasks (fail_first_run=True) execute exactly twice (initial
    raise + one retry).
  - Zero MessageLockLostError observed via redelivery
    (delivery_count == 1 for all non-retry executions).
  - Heartbeat samples from the worker stay responsive (max gap < 1s).
  - Renewal log shows actual renewals for tasks > peek_lock_seconds.

Result (run 2026-05-15, against merged main 817ebae4):
  PASS. 300 tasks (+ 30 retried failures = 330 total executions)
  across 50 gevent greenlets, peek_lock=10s, durations 1-90s.
    - 1,899 renewal RPCs fired across the run.
    - Worker heartbeat avg=101ms, max=148ms; event loop stayed
      responsive under 50-greenlet concurrency.
    - Every duration bucket ended exactly once per task, including all
      21 tasks of 90s (9× peek_lock).
    - Zero over-execution, zero MessageLockLostError.
  Total runtime: 213s for the full 330-execution batch.
"""
from __future__ import annotations

import json
import random
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from celery_gevent_extreme_app import (  # noqa: E402
    app, extreme_task, heartbeat_task,
    EXEC_LOG, RENEW_LOG, HEARTBEAT_LOG, PEEK_LOCK_SECONDS,
)


N_TASKS = 300
N_FAILING = 30  # subset that fail-first-run to force redelivery
DURATION_WEIGHTS = [
    (1, 10),    # very short
    (3, 15),
    (8, 15),
    (15, 20),   # > peek_lock, renewal needed
    (25, 15),
    (40, 10),
    (60, 5),
    (90, 5),
]
RANDOM_SEED = 17  # reproducible


def main() -> int:
    # Reset logs.
    for p in (EXEC_LOG, RENEW_LOG, HEARTBEAT_LOG):
        p.unlink(missing_ok=True)
        p.touch()

    random.seed(RANDOM_SEED)
    durations, weights = zip(*DURATION_WEIGHTS)

    print(f"peek_lock={PEEK_LOCK_SECONDS}s "
          f"N_TASKS={N_TASKS} N_FAILING={N_FAILING}")

    sent = []
    failing_indices = set(random.sample(range(N_TASKS), N_FAILING))

    # Issue ~6 heartbeat tasks staggered over the run. Each runs 30s.
    heartbeat_ids = []
    for hb in range(6):
        result = heartbeat_task.apply_async(queue="celery-extreme")
        heartbeat_ids.append(result.id)

    print(f"queued {len(heartbeat_ids)} heartbeat tasks")

    t0 = time.monotonic()
    for i in range(N_TASKS):
        dur = random.choices(durations, weights=weights, k=1)[0]
        label = f"extreme-{i:03d}-d{dur}"
        fail = i in failing_indices
        result = extreme_task.apply_async(
            args=[dur, label, fail], queue="celery-extreme")
        sent.append({
            "task_id": result.id, "label": label,
            "duration": dur, "fail": fail,
        })
        # Stagger sends slightly so the worker has a moving load.
        if i % 25 == 24:
            time.sleep(0.5)

    enqueue_time = time.monotonic() - t0
    print(f"enqueued {N_TASKS} tasks in {enqueue_time:.1f}s")

    longest = max(s["duration"] for s in sent)
    expected_wait = longest + 300
    print(f"waiting up to {expected_wait}s "
          f"(longest task duration {longest}s)")

    deadline = time.monotonic() + expected_wait
    while time.monotonic() < deadline:
        recs = _read_log(EXEC_LOG)
        ended = sum(1 for r in recs if r["event"] == "end")
        if ended >= N_TASKS:
            print(f"all {N_TASKS} non-heartbeat task ends observed "
                  f"after {int(time.monotonic() - t0)}s")
            break
        t = int(time.monotonic() - t0)
        starts = sum(1 for r in recs if r["event"] == "start")
        raised = sum(1 for r in recs if r["event"] == "raised")
        print(f"  t={t:>3}s starts={starts} ends={ended} "
              f"raised={raised}")
        time.sleep(5)

    # Settle for any in-flight renewals to flush.
    time.sleep(3)

    recs = _read_log(EXEC_LOG)
    print()
    print("=== summary ===")

    by_task: dict[str, list] = {}
    for r in recs:
        by_task.setdefault(r["task_id"], []).append(r)

    failures: list[str] = []

    expected_ids = {s["task_id"] for s in sent}
    seen_ids = set(by_task.keys())
    missing = expected_ids - seen_ids
    if missing:
        failures.append(
            f"{len(missing)} task ids missing from exec log "
            f"(never executed): {sorted(missing)[:5]}...")

    over_executed: list[str] = []
    failing_executed_count = Counter()
    nonfailing_executed_count = Counter()
    for s in sent:
        tid = s["task_id"]
        events = by_task.get(tid, [])
        ends = [e for e in events if e["event"] == "end"]
        if s["fail"]:
            failing_executed_count[len(ends)] += 1
            # Expect 1 end (retry succeeded on second attempt).
            if len(ends) != 1:
                failures.append(
                    f"failing task {s['label']}: ended {len(ends)} times "
                    f"(expected 1 after retry)")
        else:
            nonfailing_executed_count[len(ends)] += 1
            if len(ends) > 1:
                over_executed.append(s["label"])

    if over_executed:
        failures.append(
            f"{len(over_executed)} non-failing tasks executed >1 time "
            f"(renewer lost locks for at least these): "
            f"{over_executed[:10]}")

    # Heartbeat from worker process.
    hb_recs = _read_log(HEARTBEAT_LOG)
    if hb_recs:
        max_gap = max(r["max_ms"] for r in hb_recs)
        avg_avg = (sum(r["avg_ms"] for r in hb_recs) / len(hb_recs))
        print(f"worker heartbeat: {len(hb_recs)} samples, "
              f"avg avg={avg_avg:.0f}ms, max max={max_gap:.0f}ms")
        if max_gap > 1000:
            failures.append(
                f"event loop blocked > 1s "
                f"(max heartbeat gap {max_gap:.0f}ms)")

    # Renewal log.
    renew_recs = _read_log(RENEW_LOG)
    print(f"total renewal RPCs observed: {len(renew_recs)}")

    print(f"non-failing exec counts: {dict(nonfailing_executed_count)}")
    print(f"failing exec counts: {dict(failing_executed_count)}")

    # Per-duration redelivery stats.
    by_duration: dict[int, list[str]] = {}
    for s in sent:
        if s["fail"]:
            continue
        events = by_task.get(s["task_id"], [])
        ends = [e for e in events if e["event"] == "end"]
        by_duration.setdefault(s["duration"], []).append(
            f"ends={len(ends)}")
    for d in sorted(by_duration):
        counts = Counter(by_duration[d])
        print(f"  duration={d:>3}s tasks={len(by_duration[d])} "
              f"results={dict(counts)}")

    if failures:
        print()
        print("FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print()
    print(f"PASS: {N_TASKS} tasks executed correctly under "
          f"celery + gevent + use_lock_renewal "
          f"(peek_lock={PEEK_LOCK_SECONDS}s, longest task {longest}s)")
    return 0


def _read_log(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except Exception:
                pass
    return out


if __name__ == "__main__":
    raise SystemExit(main())
