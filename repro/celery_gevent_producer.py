"""Send a batch of long_task jobs to the celery worker and verify
idempotency.

Run BEFORE this:
  docker compose -f repro/docker-compose.yml up -d
  # In another shell, start the worker:
  python repro/celery_gevent_worker.py

Then:
  python repro/celery_gevent_producer.py

Result (run 2026-05-15, against merged main 817ebae4):
  PASS. 5 tasks sent (durations 3/8/15/25/30s, peek_lock=10s).
  Worker: -P gevent -c 4. All 5 tasks executed exactly once. The
  three tasks (15/25/30s) that exceed peek_lock would have been
  redelivered if the renewer failed; none were.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from celery_gevent_app import app, long_task, EXEC_LOG, PEEK_LOCK_SECONDS


# Durations span: well under peek_lock (no renewal needed),
# moderately over (1-2 renewals needed), and well over (many renewals).
DURATIONS = [3, 8, 15, 25, 30]  # seconds
LABEL_PREFIX = "celery-gevent-"


def main() -> int:
    EXEC_LOG.unlink(missing_ok=True)
    EXEC_LOG.touch()

    print(f"peek_lock_seconds={PEEK_LOCK_SECONDS}; "
          f"task durations={DURATIONS}")
    print("Sending tasks...")

    sent = []
    for i, dur in enumerate(DURATIONS):
        label = f"{LABEL_PREFIX}{i}-d{dur}"
        result = long_task.delay(dur, label)
        sent.append((result.id, label, dur))
        print(f"  sent task_id={result.id} duration={dur}s label={label}")

    # Wait long enough for all tasks to complete with margin.
    margin = 30
    wait_for = max(DURATIONS) + margin
    print(f"Waiting up to {wait_for}s for tasks to complete...")
    deadline = time.monotonic() + wait_for
    while time.monotonic() < deadline:
        recs = _read_log()
        ended = {r["task_id"] for r in recs if r["event"] == "end"}
        if len(ended) >= len(sent):
            print(f"All {len(sent)} tasks reported end at "
                  f"t={int(wait_for - (deadline - time.monotonic()))}s")
            break
        time.sleep(2)

    time.sleep(2)
    recs = _read_log()

    # Group records by task_id.
    by_task: dict[str, list] = {}
    for r in recs:
        by_task.setdefault(r["task_id"], []).append(r)

    print()
    print("=== per-task summary ===")
    failures = []
    expected_ids = {tid for tid, _, _ in sent}
    seen_ids = set(by_task.keys())
    missing = expected_ids - seen_ids
    if missing:
        failures.append(f"never executed: {sorted(missing)}")

    for tid, label, dur in sent:
        events = by_task.get(tid, [])
        starts = [e for e in events if e["event"] == "start"]
        ends = [e for e in events if e["event"] == "end"]
        print(f"  {label} (id={tid[:8]}...): "
              f"starts={len(starts)} ends={len(ends)} expected dur={dur}s")
        if len(starts) != 1:
            failures.append(
                f"{label}: {len(starts)} starts (expected 1); "
                f"likely broker redelivery, renewer failed")
        if len(ends) != 1:
            failures.append(
                f"{label}: {len(ends)} ends (expected 1)")

    # Unexpected task ids (from redelivery) get caught above too.
    extra = seen_ids - expected_ids
    if extra:
        failures.append(f"unexpected task ids in log: {sorted(extra)}")

    if failures:
        print()
        print("FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print()
    print(f"PASS: all {len(sent)} tasks executed exactly once; "
          f"renewer kept locks alive across handlers up to "
          f"{max(DURATIONS)}s with peek_lock={PEEK_LOCK_SECONDS}s")
    return 0


def _read_log() -> list[dict]:
    if not EXEC_LOG.exists():
        return []
    out = []
    for line in EXEC_LOG.read_text().splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except Exception:
                pass
    return out


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    raise SystemExit(main())
