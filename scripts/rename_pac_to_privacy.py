#!/usr/bin/env python3
"""Rename pac → privacy across the codebase.

Shared infrastructure files/identifiers/keywords get renamed.
PAC-mechanism-specific files keep their pac_ prefix (they implement the PAC
algorithm, not the general extension scaffolding).

Run with --dry-run to preview, --apply to execute.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Files to rename via `git mv` (shared infrastructure only).
FILE_RENAMES: list[tuple[str, str]] = [
    ("src/core/pac_extension.cpp", "src/core/privacy_extension.cpp"),
    ("src/include/pac_extension.hpp", "src/include/privacy_extension.hpp"),
    ("src/core/pac_optimizer.cpp", "src/core/privacy_optimizer.cpp"),
    ("src/include/core/pac_optimizer.hpp", "src/include/core/privacy_optimizer.hpp"),
    ("src/utils/pac_helpers.cpp", "src/utils/privacy_helpers.cpp"),
    ("src/include/utils/pac_helpers.hpp", "src/include/utils/privacy_helpers.hpp"),
    ("src/parser/pac_parser.cpp", "src/parser/privacy_parser.cpp"),
    ("src/parser/pac_parser_helpers.cpp", "src/parser/privacy_parser_helpers.cpp"),
    ("src/include/parser/pac_parser.hpp", "src/include/parser/privacy_parser.hpp"),
    ("src/include/parser/pac_parser_helpers.hpp", "src/include/parser/privacy_parser_helpers.hpp"),
    ("src/metadata/pac_compatibility_check.cpp", "src/metadata/privacy_compatibility_check.cpp"),
    ("src/include/metadata/pac_compatibility_check.hpp", "src/include/metadata/privacy_compatibility_check.hpp"),
    ("src/metadata/pac_metadata_manager.cpp", "src/metadata/privacy_metadata_manager.cpp"),
    ("src/include/metadata/pac_metadata_manager.hpp", "src/include/metadata/privacy_metadata_manager.hpp"),
    ("src/metadata/pac_metadata_serialization.cpp", "src/metadata/privacy_metadata_serialization.cpp"),
    ("src/include/metadata/pac_metadata_serialization.hpp", "src/include/metadata/privacy_metadata_serialization.hpp"),
    ("src/include/pac_debug.hpp", "src/include/privacy_debug.hpp"),
    ("src/test/test_pac_parser.cpp", "src/test/test_privacy_parser.cpp"),
    ("test/sql/pac_dp_elastic.test", "test/sql/dp_elastic.test"),
    ("test/sql/pac_compatible.test", "test/sql/privacy_compatible.test"),
    ("test/sql/pac_parser.test", "test/sql/privacy_parser.test"),
    ("test/sql/pac_links.test", "test/sql/privacy_links.test"),
    ("test/sql/pac_utility_threshold.test", "test/sql/privacy_min_group_count.test"),
]

# Substring replacements — applied longest-first to avoid prefix collisions.
# Each pair is (old, new). These are simple substring replacements (no word boundary).
SUBSTRING_REPLACEMENTS: list[tuple[str, str]] = [
    # File-path stems (longer compound names first)
    ("pac_metadata_serialization", "privacy_metadata_serialization"),
    ("pac_metadata_manager", "privacy_metadata_manager"),
    ("pac_compatibility_check", "privacy_compatibility_check"),
    ("pac_parser_helpers", "privacy_parser_helpers"),
    ("pac_utility_threshold", "privacy_min_group_count"),
    ("pac_extension", "privacy_extension"),
    ("pac_optimizer", "privacy_optimizer"),
    ("pac_helpers", "privacy_helpers"),
    ("pac_parser", "privacy_parser"),
    ("pac_debug", "privacy_debug"),
    # Pragma names
    ("save_pac_metadata", "save_privacy_metadata"),
    ("load_pac_metadata", "load_privacy_metadata"),
    ("clear_pac_metadata", "clear_privacy_metadata"),
    # Sidecar filename pattern (after the manager/serialization renames above)
    ("pac_metadata_", "privacy_metadata_"),
    # Macros (longest first)
    ("PAC_DEBUG_PRINT", "PRIVACY_DEBUG_PRINT"),
    ("PAC_DEBUG", "PRIVACY_DEBUG"),
    # PascalCase identifiers
    ("PACMetadataManager", "PrivacyMetadataManager"),
    ("PACTableMetadata", "PrivacyTableMetadata"),
    ("PACCompatibilityResult", "PrivacyCompatibilityResult"),
    ("PACParserExtension", "PrivacyParserExtension"),
    ("PACOptimizerExtension", "PrivacyOptimizerExtension"),
    ("PACMetadataPragma", "PrivacyMetadataPragma"),
    ("SavePACMetadata", "SavePrivacyMetadata"),
    ("LoadPACMetadata", "LoadPrivacyMetadata"),
    ("ClearPACMetadata", "ClearPrivacyMetadata"),
    ("PacExtension", "PrivacyExtension"),
    # Helper function names
    ("GetPacSeed", "GetPrivacySeed"),
    ("GetPacNoise", "GetPrivacyNoise"),
    ("GetPacUtilityThreshold", "GetPrivacyMinGroupCount"),
    # DDL keywords (uppercase)
    ("PAC_KEY", "PRIVACY_KEY"),
    ("PAC_LINK", "PRIVACY_LINK"),
]

# Word-boundary replacements (regex). Used where substring would over-match.
# Format: (pattern, replacement). Pattern is wrapped in \b...\b automatically.
WORD_BOUNDARY_REPLACEMENTS: list[tuple[str, str]] = [
    # `pac_noise` is a substring of `pac_noised_*` (PAC aggregate prefix), so we need
    # word-boundary. After this, `pac_noised_*` is untouched (correct: PAC-specific).
    (r"pac_noise", "privacy_noise"),
    (r"pac_seed", "privacy_seed"),
]

# Raw regex replacements (no auto word-boundary wrap).
# Use for context-specific patterns where the bare token "pac" is the extension name
# rather than the mode value. We must NOT rewrite `Value("pac")` (mode default),
# `== "pac"` (mode comparison), or `return "pac"` in pac_helpers.cpp (mode default).
RAW_REGEX_REPLACEMENTS: list[tuple[str, str]] = [
    # CMake / Make extension name targets
    (r"set\(TARGET_NAME pac\)", "set(TARGET_NAME privacy)"),
    (r"EXT_NAME=pac\b", "EXT_NAME=privacy"),
    # SQL: `LOAD pac` → `LOAD privacy` (extension name in DuckDB load syntax)
    (r"\bLOAD pac\b", "LOAD privacy"),
    # Built extension path: extension/pac/pac.duckdb_extension
    (r"extension/pac/pac\.duckdb_extension", "extension/privacy/privacy.duckdb_extension"),
]

# Globs of files to scan for content replacements.
SCAN_GLOBS = [
    "src/**/*.cpp",
    "src/**/*.hpp",
    "src/**/*.h",
    "test/**/*.test",
    "test/**/*.cpp",
    "test/**/*.hpp",
    "CMakeLists.txt",
    "Makefile",
    "extension_config.cmake",
    ".github/**/*.yml",
    ".github/**/*.yaml",
    "README.md",
    "CLAUDE.md",
    "docs/**/*.md",
    "benchmark/**/*.cpp",
    "benchmark/**/*.hpp",
    "benchmark/**/*.py",
    "benchmark/**/*.sh",
    ".claude/skills/explain-pac/SKILL.md",
    ".claude/skills/explain-pac-ddl/SKILL.md",
    ".claude/skills/explain-dp/SKILL.md",
    ".claude/skills/run-attacks/SKILL.md",
    ".claude/skills/test-clip/SKILL.md",
]

# Paths to skip absolutely.
SKIP_PARTS = {"duckdb", "build", ".git", "node_modules"}


def collect_files() -> list[Path]:
    files: set[Path] = set()
    for pattern in SCAN_GLOBS:
        for p in REPO.glob(pattern):
            if not p.is_file():
                continue
            if any(part in SKIP_PARTS for part in p.parts):
                continue
            files.add(p)
    return sorted(files)


def apply_replacements(text: str) -> tuple[str, int]:
    n = 0
    for old, new in SUBSTRING_REPLACEMENTS:
        if old in text:
            n += text.count(old)
            text = text.replace(old, new)
    for pat, repl in WORD_BOUNDARY_REPLACEMENTS:
        regex = re.compile(rf"\b{pat}\b")
        new_text, count = regex.subn(repl, text)
        n += count
        text = new_text
    for pat, repl in RAW_REGEX_REPLACEMENTS:
        regex = re.compile(pat)
        new_text, count = regex.subn(repl, text)
        n += count
        text = new_text
    return text, n


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, cwd=REPO)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Execute (default is dry-run).")
    args = parser.parse_args()

    dry_run = not args.apply

    # 1) Content rewrites
    files = collect_files()
    total_subs = 0
    changed_files: list[Path] = []
    for f in files:
        try:
            original = f.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        new_text, n = apply_replacements(original)
        if n > 0:
            total_subs += n
            changed_files.append(f)
            if not dry_run:
                f.write_text(new_text, encoding="utf-8")

    print(f"Content rewrites: {total_subs} substitutions across {len(changed_files)} files")
    if dry_run:
        for f in changed_files[:30]:
            print(f"  would edit: {f.relative_to(REPO)}")
        if len(changed_files) > 30:
            print(f"  ... and {len(changed_files) - 30} more")

    # 2) File renames via git mv
    print(f"\nFile renames: {len(FILE_RENAMES)} pairs")
    for old, new in FILE_RENAMES:
        old_p = REPO / old
        new_p = REPO / new
        if not old_p.exists():
            if new_p.exists():
                print(f"  skip (already renamed): {old} -> {new}")
            else:
                print(f"  WARN missing: {old}")
            continue
        if dry_run:
            print(f"  would mv: {old} -> {new}")
        else:
            new_p.parent.mkdir(parents=True, exist_ok=True)
            run(["git", "mv", old, new])
            print(f"  mv: {old} -> {new}")

    if dry_run:
        print("\nDry run only. Re-run with --apply to execute.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
