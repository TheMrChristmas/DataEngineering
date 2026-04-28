from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from reader import describe_input_file


logging.basicConfig(
    level=os.getenv("STREAM_WATCHER_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


INPUT_DIR = Path(os.getenv("LIVE_INPUT_DIR", "/opt/airflow/data/raw"))
ARCHIVE_DIR_NAME = os.getenv("LIVE_ARCHIVE_DIR_NAME", "archive")
DAG_ID = os.getenv("LIVE_DAG_ID", "hospital_admissions_pipeline")
INPUT_EXTENSION = os.getenv("LIVE_INPUT_EXTENSION", ".csv").lower()
DEBOUNCE_SECONDS = float(os.getenv("INPUT_FILE_DEBOUNCE_SECONDS", "2"))
MIN_STABLE_SECONDS = int(os.getenv("INPUT_FILE_MIN_AGE_SECONDS", "15"))
TRIGGER_DEDUP_SECONDS = float(os.getenv("TRIGGER_DEDUP_SECONDS", "30"))


class InputFileEventHandler(FileSystemEventHandler):
    def __init__(self, pending_paths: dict[str, float]) -> None:
        self.pending_paths = pending_paths

    def on_created(self, event: FileSystemEvent) -> None:
        self._mark_pending(event)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._mark_pending(event)

    def on_moved(self, event: FileSystemEvent) -> None:
        self._mark_pending(event, use_destination=True)

    def _mark_pending(
        self,
        event: FileSystemEvent,
        *,
        use_destination: bool = False,
    ) -> None:
        raw_path = getattr(event, "dest_path", "") if use_destination else event.src_path
        path = Path(raw_path)
        if event.is_directory:
            return
        if path.suffix.lower() != INPUT_EXTENSION:
            return
        if ARCHIVE_DIR_NAME in path.parts:
            return

        self.pending_paths[str(path)] = time.time()
        logger.info("Queued input file event for %s", path)


def _run_airflow_command(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["airflow", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _ensure_dag_is_unpaused() -> None:
    try:
        result = _run_airflow_command(["dags", "unpause", DAG_ID])
        if result.stdout.strip():
            logger.info(result.stdout.strip())
    except subprocess.CalledProcessError as exc:
        logger.warning("Could not unpause DAG %s: %s", DAG_ID, exc.stderr.strip())


def _trigger_dag_for_file(
    input_path: Path,
    source_snapshot: dict[str, object],
) -> None:
    run_id = (
        "event__"
        f"{input_path.stem}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    )
    conf = {
        "source_file": str(input_path),
        "source_snapshot": source_snapshot,
    }

    result = _run_airflow_command(
        ["dags", "trigger", "--run-id", run_id, "--conf", json.dumps(conf), DAG_ID]
    )
    if result.stdout.strip():
        logger.info(result.stdout.strip())


def _file_is_ready(input_path: Path) -> bool:
    try:
        age_seconds = time.time() - input_path.stat().st_mtime
    except FileNotFoundError:
        return False

    return age_seconds >= MIN_STABLE_SECONDS


def _seed_existing_files(pending_paths: dict[str, float]) -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    for path in sorted(INPUT_DIR.glob(f"*{INPUT_EXTENSION}")):
        if ARCHIVE_DIR_NAME in path.parts:
            continue
        pending_paths[str(path)] = 0.0


def main() -> int:
    pending_paths: dict[str, float] = {}
    recent_triggers: dict[tuple[str, int, int, str], float] = {}

    _ensure_dag_is_unpaused()
    _seed_existing_files(pending_paths)

    event_handler = InputFileEventHandler(pending_paths)
    observer = Observer()
    observer.schedule(event_handler, str(INPUT_DIR), recursive=False)
    observer.start()

    logger.info("Watching %s for %s files", INPUT_DIR, INPUT_EXTENSION)

    try:
        while True:
            now = time.time()
            recent_triggers = {
                source_key: triggered_at
                for source_key, triggered_at in recent_triggers.items()
                if now - triggered_at < TRIGGER_DEDUP_SECONDS
            }
            ready_paths = [
                Path(path)
                for path, queued_at in list(pending_paths.items())
                if now - queued_at >= DEBOUNCE_SECONDS
            ]

            for input_path in ready_paths:
                pending_paths.pop(str(input_path), None)
                if not input_path.exists():
                    continue
                if not _file_is_ready(input_path):
                    pending_paths[str(input_path)] = time.time()
                    continue

                try:
                    source_snapshot = describe_input_file(str(input_path))
                except FileNotFoundError:
                    continue

                source_key = (
                    str(input_path),
                    int(source_snapshot.get("modified_at_ns", 0)),
                    int(source_snapshot.get("size_bytes", 0)),
                    str(source_snapshot.get("sha256", "")),
                )
                if source_key in recent_triggers:
                    logger.info("Ignoring duplicate file event for %s", input_path)
                    continue

                _trigger_dag_for_file(input_path, source_snapshot)
                recent_triggers[source_key] = now
                logger.info("Triggered DAG %s for %s", DAG_ID, input_path)

            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping file watcher")
    finally:
        observer.stop()
        observer.join()

    return 0


if __name__ == "__main__":
    sys.exit(main())
