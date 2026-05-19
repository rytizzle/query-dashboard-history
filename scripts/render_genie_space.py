#!/usr/bin/env python3
"""
Render the Genie space templates (JSON + SQL) into a deploy-ready directory.

Substitutes three placeholders:
  {{CATALOG}}    - UC catalog containing the pipeline outputs
  {{SCHEMA}}     - UC schema containing the pipeline outputs (e.g. system_tables)
  {{MV_SCHEMA}}  - UC schema where the query_history_mv metric view is deployed (e.g. default)

Reads from   genie_space/
Writes to    genie_space/rendered/

Defaults pull from the bundle vars in databricks.yml. Override via CLI flags
or by exporting BUNDLE_VAR_catalog / BUNDLE_VAR_schema / BUNDLE_VAR_mv_schema.

Usage:
  python3 scripts/render_genie_space.py --catalog acme --schema workspace_inv --mv-schema default
  python3 scripts/render_genie_space.py            # uses defaults from databricks.yml
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "genie_space"
OUT_DIR = SRC_DIR / "rendered"
TEMPLATE_SUFFIXES = (".json", ".sql")


def _default_from_bundle_vars() -> dict[str, str]:
    """Read variable defaults out of databricks.yml without importing yaml."""
    bundle_yml = REPO_ROOT / "databricks.yml"
    defaults = {"CATALOG": "ryant_catalog", "SCHEMA": "system_tables", "MV_SCHEMA": "default"}
    if not bundle_yml.exists():
        return defaults
    text = bundle_yml.read_text()
    # Minimal YAML scrape — we only need the `default:` line right after each var name.
    lines = text.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        for var_name, key in (("catalog:", "CATALOG"), ("schema:", "SCHEMA"), ("metric_view_schema:", "MV_SCHEMA")):
            if stripped == var_name and i + 1 < len(lines):
                nxt = lines[i + 1].strip()
                if nxt.startswith("default:"):
                    val = nxt.split("default:", 1)[1].strip().strip('"').strip("'")
                    if val:
                        defaults[key] = val
    return defaults


def render(catalog: str, schema: str, mv_schema: str) -> Path:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True)

    subs = {"{{CATALOG}}": catalog, "{{SCHEMA}}": schema, "{{MV_SCHEMA}}": mv_schema}
    rendered_files: list[Path] = []
    for src in sorted(SRC_DIR.iterdir()):
        if src.is_dir() or src.suffix not in TEMPLATE_SUFFIXES:
            continue
        text = src.read_text()
        for placeholder, value in subs.items():
            text = text.replace(placeholder, value)
        dst = OUT_DIR / src.name
        dst.write_text(text)
        rendered_files.append(dst)

    print(f"Rendered {len(rendered_files)} file(s) -> {OUT_DIR.relative_to(REPO_ROOT)}/")
    for p in rendered_files:
        print(f"  {p.relative_to(REPO_ROOT)}")
    print()
    print(f"Substitutions: CATALOG={catalog}  SCHEMA={schema}  MV_SCHEMA={mv_schema}")
    return OUT_DIR


def main() -> int:
    defaults = _default_from_bundle_vars()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", default=os.environ.get("BUNDLE_VAR_catalog", defaults["CATALOG"]))
    parser.add_argument("--schema", default=os.environ.get("BUNDLE_VAR_schema", defaults["SCHEMA"]))
    parser.add_argument(
        "--mv-schema",
        default=os.environ.get("BUNDLE_VAR_metric_view_schema", defaults["MV_SCHEMA"]),
        help="Schema where the query_history_mv metric view is deployed",
    )
    args = parser.parse_args()
    render(args.catalog, args.schema, args.mv_schema)
    return 0


if __name__ == "__main__":
    sys.exit(main())
