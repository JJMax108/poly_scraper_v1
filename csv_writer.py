# csv_writer.py
# One CSV per product range with flexible headers that can grow over time
# Now with logging

from pathlib import Path
import csv
import re
from typing import Dict, List, Iterable
import logging

log = logging.getLogger("poly")

class RangeCsvWriter:
    def __init__(self, base_dir: Path, core_fields: Iterable[str]):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.core_fields = list(core_fields)
        self.schemas: Dict[str, List[str]] = {}

    @staticmethod
    def _range_key(name: str) -> str:
        key = name.strip().lower().replace(" ", "_")
        key = re.sub(r"[^a-z0-9_]+", "", key)
        return key or "unknown_range"

    def _csv_path(self, range_key: str) -> Path:
        return self.base_dir / f"{range_key}.csv"

    def _load_existing_header(self, path: Path) -> List[str]:
        if not path.exists():
            return []
        with path.open("r", newline="") as f:
            reader = csv.reader(f)
            try:
                return next(reader)
            except StopIteration:
                return []

    def _read_all_rows(self, path: Path) -> List[Dict[str, str]]:
        if not path.exists():
            return []
        with path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _write_all(self, path: Path, header: List[str], rows: List[Dict[str, str]]):
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in header})

    def _ensure_schema(self, range_name: str, spec_fields: Iterable[str]) -> List[str]:
        range_key = self._range_key(range_name)
        path = self._csv_path(range_key)

        if range_key not in self.schemas:
            existing_header = self._load_existing_header(path)
            if existing_header:
                self.schemas[range_key] = existing_header
                log.info(f"csv open existing: {path.name}")
            else:
                self.schemas[range_key] = list(self.core_fields)
                log.info(f"csv new: {path.name}")

        header = self.schemas[range_key]

        grew = False
        for sf in spec_fields:
            if sf and sf not in header:
                header.append(sf)
                grew = True

        if path.exists():
            disk_header = self._load_existing_header(path)
            if disk_header != header:
                rows = self._read_all_rows(path)
                self._write_all(path, header, rows)
                added = [c for c in header if c not in disk_header]
                if added:
                    log.info(f"csv header grew: {path.name} added={added}")

        return header

    def append_row(self, range_name: str, core: Dict[str, str], specs: Dict[str, str]):
        range_key = self._range_key(range_name)
        path = self._csv_path(range_key)

        safe_specs = {k: v for k, v in specs.items() if k not in core}
        header = self._ensure_schema(range_name, safe_specs.keys())

        row = {**{k: core.get(k, "") for k in self.core_fields}}
        for col in header:
            if col in row:
                continue
            row[col] = safe_specs.get(col, "")

        write_headers = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header)
            if write_headers:
                w.writeheader()
            w.writerow(row)

        log.info(f"csv write: {path.name} sku={core.get('sku_code','')} colour='{core.get('colour_name','')}'")