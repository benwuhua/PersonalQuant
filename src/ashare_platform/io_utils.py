from __future__ import annotations

from datetime import datetime
import json
import shutil
from pathlib import Path
from typing import Any


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with path.open('w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)


def archive_output_batch(outputs_dir: Path, archives_dir: Path, *, run_label: str | None = None) -> Path:
    ensure_dir(archives_dir)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    batch_name = run_label or f'run_{timestamp}'
    batch_dir = archives_dir / batch_name
    ensure_dir(batch_dir)

    archived_files: list[str] = []
    for path in outputs_dir.iterdir():
        if not path.is_file() or path.name.startswith('.'):
            continue
        target = batch_dir / path.name
        shutil.copy2(path, target)
        archived_files.append(path.name)

    manifest = {
        'batch_name': batch_name,
        'archived_at': datetime.now().isoformat(timespec='seconds'),
        'source_dir': str(outputs_dir),
        'files': sorted(archived_files),
    }
    write_json(batch_dir / 'manifest.json', manifest)
    write_json(archives_dir / 'latest.json', manifest)
    return batch_dir


def list_recent_archives(archives_dir: Path, limit: int = 10) -> list[dict[str, Any]]:
    if not archives_dir.exists():
        return []
    manifests: list[dict[str, Any]] = []
    for manifest_path in sorted(archives_dir.glob('*/manifest.json'), reverse=True):
        try:
            with manifest_path.open('r', encoding='utf-8') as f:
                manifests.append(json.load(f))
        except Exception:
            continue
        if len(manifests) >= limit:
            break
    return manifests
