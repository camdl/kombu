"""Boot a celery worker against the emulator under the gevent pool.

Equivalent to:
  celery -A repro.celery_gevent_app worker -P gevent -c 4 --loglevel=INFO

We invoke programmatically so this whole flow is one shell command per
process. Apply gevent monkey-patching FIRST, before any celery/kombu
imports (same constraint celery itself documents for -P gevent).
"""
from __future__ import annotations

from gevent import monkey
monkey.patch_all()  # noqa: E402  must be first

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent))

from celery_gevent_app import app  # noqa: E402


if __name__ == "__main__":
    app.worker_main(
        argv=[
            "worker",
            "-P", "gevent",
            "-c", "4",
            "--loglevel=INFO",
            "--without-heartbeat",
            "--without-gossip",
            "--without-mingle",
        ]
    )
