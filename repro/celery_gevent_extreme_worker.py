"""Boot a celery worker for the extreme test under gevent pool.

50 concurrent greenlets, prefetch=1.

Run after `docker compose -f repro/docker-compose.yml up -d` and after
the emulator has the `celery-extreme` queue (provisioned in
repro/Config.json).
"""
from __future__ import annotations

from gevent import monkey
monkey.patch_all()  # noqa: E402

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent))

from celery_gevent_extreme_app import app, CONCURRENCY  # noqa: E402


if __name__ == "__main__":
    app.worker_main(
        argv=[
            "worker",
            "-P", "gevent",
            "-c", str(CONCURRENCY),
            "-Q", "celery-extreme",
            "--loglevel=WARNING",
            "--without-heartbeat",
            "--without-gossip",
            "--without-mingle",
        ]
    )
