#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT / "webapp_data" / "reports"
REPORT_JSON = REPORT_DIR / "credential_audit.json"
REPORT_MD = REPORT_DIR / "credential_audit.md"

EXCLUDE_DIRS = {".git", "node_modules", "__pycache__", "webapp/static/assets", "webapp_data/reports"}
TARGET_EXTS = {".py", ".js", ".ts", ".jsx", ".tsx", ".json", ".yaml", ".yml", ".toml", ".ini", ".env"}


@dataclass
class Finding:
    file: str
    line: int
    kind: str
    snippet: str


PATTERNS: dict[str, re.Pattern[str]] = {
    "hardcoded_access_token": re.compile(r"access[_-]?token\s*[:=]\s*['\"][A-Za-z0-9_-]{20,}['\"]", re.I),
    "hardcoded_secret": re.compile(r"(secret|secret_key|app_secret|client_secret)\s*[:=]\s*['\"][A-Za-z0-9_-]{20,}['\"]", re.I),
    "hardcoded_shopify_pat": re.compile(r"shpat_[A-Za-z0-9]+"),
    "url_token_param": re.compile(r"https?://[^\s'\"`]+[?&]token=[A-Za-z0-9_-]{12,}", re.I),
}


def _should_scan(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    if any(rel.startswith(prefix) for prefix in EXCLUDE_DIRS):
        return False
    if rel.startswith("config/"):
        # Config is the designated secret storage and should not be treated as code leakage.
        return False
    return path.suffix.lower() in TARGET_EXTS


def scan_repo() -> list[Finding]:
    findings: list[Finding] = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or not _should_scan(path):
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for idx, line in enumerate(content.splitlines(), start=1):
            for kind, pattern in PATTERNS.items():
                if pattern.search(line):
                    findings.append(
                        Finding(
                            file=path.relative_to(ROOT).as_posix(),
                            line=idx,
                            kind=kind,
                            snippet=line.strip()[:220],
                        )
                    )
                    break
    return findings


def write_reports(findings: list[Finding]) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"total_findings": len(findings), "findings": [asdict(x) for x in findings]}
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Credential Audit Report",
        "",
        f"- total findings: **{len(findings)}**",
        "",
        "| file | line | kind | snippet |",
        "|---|---:|---|---|",
    ]
    for item in findings[:400]:
        snippet = item.snippet.replace("|", "\\|")
        lines.append(f"| `{item.file}` | {item.line} | `{item.kind}` | `{snippet}` |")
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    findings = scan_repo()
    write_reports(findings)
    print("credential audit completed")
    print("total findings:", len(findings))
    print("json report:", REPORT_JSON)
    print("md report:", REPORT_MD)


if __name__ == "__main__":
    main()
