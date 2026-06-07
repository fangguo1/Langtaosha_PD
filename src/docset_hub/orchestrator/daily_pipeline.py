"""Daily fetch, ingest, and enrichment pipeline."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class DailyPipelineConfig:
    project_root: Path
    config_path: Path
    target_date: date
    dry_run: bool = False
    run_vector_stage: bool = True
    run_author_enrichment: bool = True
    python_executable: str = sys.executable


class DailyPipeline:
    def __init__(self, config: DailyPipelineConfig) -> None:
        self.config = config
        self.project_root = config.project_root.resolve()
        self.target_day = config.target_date.isoformat()
        self.manifest_dir = self.project_root / "local_data" / "daily_orchestrator" / self.target_day
        self.logs_dir = self.manifest_dir / "logs"
        self.docs_log_dir = self.project_root / "docs" / "daily_orchestrator_log"

    def run(self) -> Dict[str, Any]:
        manifest: Dict[str, Any] = {
            "mode": "daily_pipeline",
            "target_date": self.target_day,
            "dry_run": self.config.dry_run,
            "started_at": self._utc_now(),
            "steps": [],
        }
        try:
            biorxiv_file = self._biorxiv_daily_path()
            langtaosha_file = self._langtaosha_daily_path()

            manifest["steps"].append(self._fetch_biorxiv(biorxiv_file))
            manifest["steps"].append(self._fetch_langtaosha())
            manifest["steps"].append(self._ensure_empty_file(biorxiv_file, "biorxiv_daily"))
            manifest["steps"].append(self._ensure_empty_file(langtaosha_file, "langtaosha"))
            manifest["steps"].append(self._ingest_file(biorxiv_file, "biorxiv_daily"))
            manifest["steps"].append(self._ingest_file(langtaosha_file, "langtaosha"))

            if self.config.run_author_enrichment:
                manifest["steps"].append(self._enrich_authors("biorxiv_daily"))
                manifest["steps"].append(self._enrich_authors("langtaosha"))

            manifest["status"] = "ok" if all(step["status"] in {"ok", "skipped", "dry_run"} for step in manifest["steps"]) else "partial_failure"
        except Exception as exc:
            manifest["status"] = "failed"
            manifest["error"] = str(exc)
        finally:
            manifest["finished_at"] = self._utc_now()
            self._write_json(self.manifest_dir / "manifest.json", manifest)
            self._append_docs_log(
                "daily_orchestrator",
                {
                    "event_type": "daily_pipeline_manifest",
                    "target_date": self.target_day,
                    "status": manifest.get("status"),
                    "dry_run": self.config.dry_run,
                    "started_at": manifest.get("started_at"),
                    "finished_at": manifest.get("finished_at"),
                    "manifest_path": str(self.manifest_dir / "manifest.json"),
                    "steps": [
                        {
                            "step": step.get("step"),
                            "status": step.get("status"),
                            "returncode": step.get("returncode"),
                            "record_count": step.get("record_count"),
                            "reason": step.get("reason"),
                        }
                        for step in manifest.get("steps", [])
                    ],
                    "error": manifest.get("error"),
                },
            )
        return manifest

    def _fetch_biorxiv(self, output_file: Path) -> Dict[str, Any]:
        temp_json = self.manifest_dir / "biorxiv_fetch_raw.json"
        command = [
            self.config.python_executable,
            "scripts/bioarxiv/biorxiv_api.py",
            "historical",
            "--server",
            "biorxiv",
            "--start-date",
            self.target_day,
            "--end-date",
            self.target_day,
            "--output",
            str(temp_json),
        ]
        if self.config.dry_run:
            return self._dry_step("fetch_biorxiv", command, output=str(output_file))

        result = self._run_command("fetch_biorxiv", command)
        records: List[Dict[str, Any]] = []
        if temp_json.exists():
            payload = json.loads(temp_json.read_text(encoding="utf-8"))
            records = payload.get("records") or []
        self._write_jsonl(output_file, records)
        result["output"] = str(output_file)
        result["record_count"] = len(records)
        return result

    def _fetch_langtaosha(self) -> Dict[str, Any]:
        command = [
            self.config.python_executable,
            "scripts/langtaosha/langtaosha_scrape.py",
            "--root-dir",
            str(self.project_root / "local_data" / "langtaosha"),
            "--mode",
            "daily-update",
            "--start-date",
            self.target_day,
            "--end-date",
            self.target_day,
        ]
        if self.config.dry_run:
            return self._dry_step("fetch_langtaosha", command, output=str(self._langtaosha_daily_path()))
        return self._run_command("fetch_langtaosha", command)

    def _ensure_empty_file(self, path: Path, source_name: str) -> Dict[str, Any]:
        if self.config.dry_run:
            return {"step": f"ensure_empty_{source_name}", "status": "dry_run", "path": str(path)}
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("", encoding="utf-8")
        return {
            "step": f"ensure_empty_{source_name}",
            "status": "ok",
            "path": str(path),
            "record_count": self._count_jsonl(path),
        }

    def _ingest_file(self, path: Path, source_name: str) -> Dict[str, Any]:
        if self.config.dry_run:
            return self._dry_step("ingest_" + source_name, self._ingest_command(path, source_name), input=str(path))
        if not path.exists() or path.stat().st_size == 0:
            return {"step": "ingest_" + source_name, "status": "skipped", "reason": "empty_file", "input": str(path)}
        return self._run_command("ingest_" + source_name, self._ingest_command(path, source_name))

    def _ingest_command(self, path: Path, source_name: str) -> List[str]:
        stage = "all" if self.config.run_vector_stage else "pg"
        return [
            self.config.python_executable,
            "scripts/backfill_source_records.py",
            "--config-path",
            str(self.config.config_path),
            "--records-root",
            str(path),
            "--source-name",
            source_name,
            "--stage",
            stage,
        ]

    def _enrich_authors(self, source_name: str) -> Dict[str, Any]:
        manifest_path = self.manifest_dir / f"semantic_scholar_{source_name}.json"
        jsonl_path = self.manifest_dir / f"semantic_scholar_{source_name}.jsonl"
        command = [
            self.config.python_executable,
            "scripts/backfill_semantic_scholar_authors.py",
            "--config-path",
            str(self.config.config_path),
            "--source-name",
            source_name,
            "--date",
            self.target_day,
            "--limit",
            "500",
            "--record-status",
            "--require-api-key",
            "--validate-api-key",
            "--use-batch-api",
            "--semantic-batch-size",
            "100",
            "--manifest",
            str(manifest_path),
            "--jsonl",
            str(jsonl_path),
        ]
        if self.config.dry_run:
            return self._dry_step("enrich_authors_" + source_name, command)
        result = self._run_command("enrich_authors_" + source_name, command, allow_failure=True)
        result["manifest"] = str(manifest_path)
        result["jsonl"] = str(jsonl_path)
        result["non_blocking"] = True
        self._append_author_enrichment_docs_log(source_name, result, manifest_path, jsonl_path)
        return result

    def _run_command(self, step: str, command: List[str], allow_failure: bool = False) -> Dict[str, Any]:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        completed = subprocess.run(
            command,
            cwd=self.project_root,
            text=True,
            capture_output=True,
            check=False,
        )
        (self.logs_dir / f"{step}.stdout.log").write_text(completed.stdout or "", encoding="utf-8")
        (self.logs_dir / f"{step}.stderr.log").write_text(completed.stderr or "", encoding="utf-8")
        status = "ok" if completed.returncode == 0 else ("skipped" if allow_failure else "failed")
        return {
            "step": step,
            "status": status,
            "returncode": completed.returncode,
            "command": command,
            "stdout_log": str(self.logs_dir / f"{step}.stdout.log"),
            "stderr_log": str(self.logs_dir / f"{step}.stderr.log"),
        }

    @staticmethod
    def _dry_step(step: str, command: List[str], **extra: Any) -> Dict[str, Any]:
        payload = {"step": step, "status": "dry_run", "command": command}
        payload.update(extra)
        return payload

    def _biorxiv_daily_path(self) -> Path:
        return self.project_root / "local_data" / "biorxiv_daily" / str(self.config.target_date.year) / f"{self.target_day}.jsonl"

    def _langtaosha_daily_path(self) -> Path:
        return self.project_root / "local_data" / "langtaosha" / "daily" / str(self.config.target_date.year) / f"{self.target_day}.jsonl"

    @staticmethod
    def _count_jsonl(path: Path) -> int:
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    @staticmethod
    def _write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _append_author_enrichment_docs_log(
        self,
        source_name: str,
        command_result: Dict[str, Any],
        manifest_path: Path,
        jsonl_path: Path,
    ) -> None:
        if jsonl_path.exists():
            with jsonl_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    row.update(
                        {
                            "event_type": "author_enrichment_result",
                            "target_date": self.target_day,
                            "source_name": source_name,
                            "manifest_path": str(manifest_path),
                        }
                    )
                    self._append_docs_log("author_enrichment", row)

        manifest = self._read_json(manifest_path)
        self._append_docs_log(
            "author_enrichment",
            {
                "event_type": "author_enrichment_summary",
                "target_date": self.target_day,
                "source_name": source_name,
                "status": command_result.get("status"),
                "returncode": command_result.get("returncode"),
                "manifest_path": str(manifest_path),
                "jsonl_path": str(jsonl_path),
                "total": manifest.get("total"),
                "updated": manifest.get("updated"),
                "skipped": manifest.get("skipped"),
                "failed": manifest.get("failed"),
                "status_counts": manifest.get("status_counts"),
                "api_key_present": manifest.get("api_key_present"),
                "api_key_validation": manifest.get("api_key_validation"),
            },
        )

    def _append_docs_log(self, prefix: str, row: Dict[str, Any]) -> None:
        self.docs_log_dir.mkdir(parents=True, exist_ok=True)
        log_date = self.target_day.replace("-", "")
        path = self.docs_log_dir / f"{prefix}_{log_date}.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
