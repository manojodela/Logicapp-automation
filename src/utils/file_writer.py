"""
File Writer Module
Persists generated workflow JSON to structured local directories.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils.logger import get_logger

logger = get_logger("file_writer")


class FileWriter:
    def __init__(self, config):
        self.output_dir = config.output_dir
        self.failed_dir = config.failed_dir

    def _build_path(self, base_dir: Path, workflow_name: str, attempt: int, suffix: str = "") -> Path:
        """
        Build a deterministic, non-colliding file path.

        Convention: {base_dir}/{workflow_name}/{YYYYMMDD_HHMMSS}_attempt{N}{suffix}.json
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in workflow_name)
        workflow_dir = base_dir / safe_name
        workflow_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{ts}_attempt{attempt:02d}{suffix}.json"
        return workflow_dir / filename

    def save(self, workflow_json: dict, workflow_name: str, attempt: int) -> Path:
        """Save a validated workflow definition to the generated output directory."""
        path = self._build_path(self.output_dir, workflow_name, attempt)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(workflow_json, f, indent=2)
        logger.info("Workflow persisted", extra={"path": str(path)})
        return path

    def save_failed(self, workflow_json: dict, workflow_name: str, attempt: int, errors: list) -> Path:
        """Save a failed workflow definition (with error metadata) to the failed directory."""
        path = self._build_path(self.failed_dir, workflow_name, attempt, suffix="_FAILED")
        payload = {
            "_metadata": {
                "workflow_name": workflow_name,
                "attempt": attempt,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "errors": errors,
            },
            "workflow": workflow_json,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.warning("Failed workflow persisted for triage", extra={"path": str(path)})
        return path
