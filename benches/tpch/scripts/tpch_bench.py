#!/usr/bin/env python3
"""Generate, load, and compare a TPC-H-style workload on PostgreSQL and pg_fusion."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Iterable


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEMA_SQL = ROOT / "schema.sql"
QUERIES_DIR = ROOT / "queries"
TABLES = [
    "region",
    "nation",
    "supplier",
    "part",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
]
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclasses.dataclass
class QueryRun:
    ok: bool
    times_ms: list[float]
    row_counts: list[int]
    hashes: list[str]
    stdout: str | None = None
    error: str | None = None

    @property
    def median_ms(self) -> float | None:
        if not self.ok or not self.times_ms:
            return None
        return float(statistics.median(self.times_ms))

    @property
    def representative_hash(self) -> str | None:
        return self.hashes[-1] if self.hashes else None

    @property
    def representative_rows(self) -> int | None:
        return self.row_counts[-1] if self.row_counts else None


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    root = ROOT.resolve()
    data_dir = pathlib.Path(args.data_dir or root / "data" / f"sf_{scale_label(args.scale_factor)}")
    results_dir = pathlib.Path(args.results_dir or root / "results")
    results_dir.mkdir(parents=True, exist_ok=True)

    psql = find_psql(args.psql)
    tpchgen = find_binary(args.tpchgen, "tpchgen-cli", "TPCHGEN")
    queries = select_queries(args.queries)

    if not args.no_prepare:
        generate_data(tpchgen, args.scale_factor, data_dir, args.force_generate)
        load_data(psql, args, data_dir, results_dir)
        if args.only_prepare:
            print(f"Prepared schema {args.schema!r} from {data_dir}")
            return 0

    check_connection(psql, args)
    results = run_suite(psql, args, queries, results_dir)
    write_results(results, args, results_dir)
    print_summary(results)
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run pg_fusion against a TPC-H-style diagnostic workload."
    )
    parser.add_argument("--scale-factor", "-s", default="0.01", help="tpchgen scale factor")
    parser.add_argument("--schema", default="tpch", help="target PostgreSQL schema")
    parser.add_argument("--queries", default="all", help="comma list like q01,q06 or 'all'")
    parser.add_argument("--runs", type=int, default=3, help="measured runs per query/mode")
    parser.add_argument("--warmup", type=int, default=1, help="warmup runs per query/mode")
    parser.add_argument("--timeout", type=float, default=120.0, help="per query timeout in seconds")
    parser.add_argument("--parallel-workers", type=int, default=2)
    parser.add_argument("--float-abs-tolerance", type=float, default=1e-6)
    parser.add_argument("--float-rel-tolerance", type=float, default=1e-9)
    parser.add_argument("--no-prepare", action="store_true", help="skip data generation and loading")
    parser.add_argument("--only-prepare", action="store_true", help="prepare data/schema and exit")
    parser.add_argument("--force-generate", action="store_true", help="regenerate CSV files")
    parser.add_argument("--data-dir", help="directory containing/generated CSV files")
    parser.add_argument("--results-dir", help="directory for CSV/JSON result summaries")
    parser.add_argument("--psql", help="path to psql; also reads $PSQL")
    parser.add_argument("--tpchgen", help="path to tpchgen-cli; also reads $TPCHGEN")
    parser.add_argument("--dbname", "-d", help="database name or conninfo string")
    parser.add_argument("--host", help="PostgreSQL host")
    parser.add_argument("--port", help="PostgreSQL port")
    parser.add_argument("--user", help="PostgreSQL user")
    parsed = parser.parse_args(argv)

    if parsed.runs < 1:
        parser.error("--runs must be positive")
    if parsed.warmup < 0:
        parser.error("--warmup must be non-negative")
    if parsed.parallel_workers < 0:
        parser.error("--parallel-workers must be non-negative")
    if not IDENT_RE.match(parsed.schema):
        parser.error("--schema must be a simple SQL identifier")
    return parsed


def scale_label(scale: str) -> str:
    return scale.replace(".", "_").replace("-", "_")


def find_binary(explicit: str | None, name: str, env_name: str) -> pathlib.Path:
    candidates = [explicit, os.environ.get(env_name), shutil.which(name)]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return pathlib.Path(candidate)
    raise SystemExit(f"Could not find {name}; pass --{name.split('-')[0]} or set ${env_name}")


def find_psql(explicit: str | None) -> pathlib.Path:
    candidates: list[str | None] = [explicit, os.environ.get("PSQL"), shutil.which("psql")]
    pgrx_root = pathlib.Path.home() / ".pgrx"
    if pgrx_root.exists():
        pgrx_psqls = sorted(
            pgrx_root.glob("*/pgrx-install/bin/psql"),
            key=lambda path: version_key(path.parts[-4]),
            reverse=True,
        )
        candidates.extend(str(path) for path in pgrx_psqls)
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return pathlib.Path(candidate)
    raise SystemExit("Could not find psql; pass --psql or set $PSQL")


def version_key(value: str) -> tuple[int, ...]:
    parts = []
    for item in value.split("."):
        try:
            parts.append(int(item))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def psql_base_cmd(psql: pathlib.Path, args: argparse.Namespace) -> list[str]:
    cmd = [str(psql), "-X", "-q", "-v", "ON_ERROR_STOP=1"]
    dbname = args.dbname or os.environ.get("DATABASE_URL")
    detected_host, detected_port = detect_pgrx_socket()
    if dbname:
        cmd.extend(["-d", dbname])
    host = args.host or os.environ.get("PGHOST") or detected_host
    port = args.port or os.environ.get("PGPORT") or detected_port
    if host:
        cmd.extend(["-h", host])
    if port:
        cmd.extend(["-p", port])
    if args.user:
        cmd.extend(["-U", args.user])
    return cmd


def detect_pgrx_socket() -> tuple[str | None, str | None]:
    pgrx_root = pathlib.Path.home() / ".pgrx"
    if os.environ.get("PGHOST") or os.environ.get("PGPORT") or not pgrx_root.exists():
        return None, None
    sockets = sorted(
        path
        for path in pgrx_root.glob(".s.PGSQL.*")
        if not path.name.endswith(".lock") and path.name.rsplit(".", 1)[-1].isdigit()
    )
    if len(sockets) != 1:
        return None, None
    port = sockets[0].name.rsplit(".", 1)[-1]
    return str(pgrx_root), port


def run_psql_file(
    psql: pathlib.Path,
    args: argparse.Namespace,
    sql_path: pathlib.Path,
    *,
    capture: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        psql_base_cmd(psql, args) + ["-f", str(sql_path)],
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        timeout=timeout,
        check=False,
    )


def run_psql_sql(
    psql: pathlib.Path,
    args: argparse.Namespace,
    sql: str,
    results_dir: pathlib.Path,
    *,
    unaligned: bool = False,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    with tempfile.NamedTemporaryFile(
        "w", suffix=".sql", prefix="tpch_", dir=results_dir, delete=False
    ) as handle:
        handle.write(sql)
        temp_path = pathlib.Path(handle.name)
    try:
        cmd = psql_base_cmd(psql, args)
        if unaligned:
            cmd.extend(["-A", "-t", "-F", "\t"])
        return subprocess.run(
            cmd + ["-f", str(temp_path)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    finally:
        temp_path.unlink(missing_ok=True)


def generate_data(
    tpchgen: pathlib.Path,
    scale_factor: str,
    data_dir: pathlib.Path,
    force: bool,
) -> None:
    expected = [data_dir / f"{table}.csv" for table in TABLES]
    if not force and all(path.exists() for path in expected):
        print(f"Using existing TPC-H CSV data in {data_dir}")
        return
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(tpchgen),
        "-s",
        str(scale_factor),
        "--format=csv",
        "--output-dir",
        str(data_dir),
    ]
    print("Generating TPC-H CSV data:", " ".join(cmd))
    completed = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if completed.returncode != 0:
        raise SystemExit(completed.stderr.strip() or "tpchgen-cli failed")


def load_data(
    psql: pathlib.Path,
    args: argparse.Namespace,
    data_dir: pathlib.Path,
    results_dir: pathlib.Path,
) -> None:
    missing = [str(data_dir / f"{table}.csv") for table in TABLES if not (data_dir / f"{table}.csv").exists()]
    if missing:
        raise SystemExit("Missing generated CSV files:\n" + "\n".join(missing))

    schema_sql = SCHEMA_SQL.read_text().replace("__TPCH_SCHEMA__", args.schema)
    copy_lines = [
        "\\copy {schema}.{table} FROM '{path}' WITH (FORMAT csv, HEADER true)".format(
            schema=args.schema,
            table=table,
            path=str((data_dir / f"{table}.csv").resolve()).replace("'", "''"),
        )
        for table in TABLES
    ]
    analyze_lines = [f"ANALYZE {args.schema}.{table};" for table in TABLES]
    sql = schema_sql + "\n" + "\n".join(copy_lines) + "\n" + "\n".join(analyze_lines) + "\n"
    load_file = results_dir / "load.sql"
    load_file.write_text(sql)
    print(f"Loading schema {args.schema!r} into PostgreSQL")
    completed = run_psql_file(psql, args, load_file, timeout=max(args.timeout, 30.0))
    if completed.returncode != 0:
        raise SystemExit((completed.stderr or completed.stdout).strip())


def check_connection(psql: pathlib.Path, args: argparse.Namespace) -> None:
    sql = "SELECT 1;\nSHOW shared_preload_libraries;\n"
    with tempfile.TemporaryDirectory(prefix="tpch_check_") as tmp:
        completed = run_psql_sql(
            psql,
            args,
            sql,
            pathlib.Path(tmp),
            unaligned=True,
            timeout=min(args.timeout, 10.0),
        )
    if completed.returncode != 0:
        raise SystemExit((completed.stderr or completed.stdout).strip())
    if "pg_fusion" not in completed.stdout:
        print(
            "warning: shared_preload_libraries does not mention pg_fusion; "
            "fusion runs may use vanilla PostgreSQL",
            file=sys.stderr,
        )


def select_queries(selection: str) -> list[pathlib.Path]:
    files = sorted(QUERIES_DIR.glob("q*.sql"))
    if selection == "all":
        return files
    wanted = {item.strip().lower() for item in selection.split(",") if item.strip()}
    selected = [path for path in files if path.stem.lower() in wanted]
    missing = sorted(wanted - {path.stem.lower() for path in selected})
    if missing:
        raise SystemExit(f"Unknown query id(s): {', '.join(missing)}")
    return selected


def run_suite(
    psql: pathlib.Path,
    args: argparse.Namespace,
    queries: Iterable[pathlib.Path],
    results_dir: pathlib.Path,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for query_path in queries:
        query_id = query_path.stem
        query = query_path.read_text()
        print(f"Running {query_id}")
        pg = run_query_mode(psql, args, query, "off", results_dir)
        fusion = run_query_mode(psql, args, query, "on", results_dir)
        rows.append(summarize_query(query_id, pg, fusion, args))
    return rows


def run_query_mode(
    psql: pathlib.Path,
    args: argparse.Namespace,
    query: str,
    fusion_enable: str,
    results_dir: pathlib.Path,
) -> QueryRun:
    times: list[float] = []
    row_counts: list[int] = []
    hashes: list[str] = []
    stdout: str | None = None
    total_runs = args.warmup + args.runs
    for index in range(total_runs):
        sql = render_query(args, query, fusion_enable)
        start = time.perf_counter()
        try:
            completed = run_psql_sql(
                psql,
                args,
                sql,
                results_dir,
                unaligned=True,
                timeout=args.timeout + 5.0,
            )
        except subprocess.TimeoutExpired:
            return QueryRun(False, times, row_counts, hashes, stdout, f"timeout after {args.timeout:.1f}s")
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        if completed.returncode != 0:
            error = (completed.stderr or completed.stdout).strip()
            return QueryRun(False, times, row_counts, hashes, stdout, tail(error))
        if index >= args.warmup:
            stdout = completed.stdout
            payload = completed.stdout.encode()
            times.append(elapsed_ms)
            hashes.append(hashlib.sha256(payload).hexdigest())
            row_counts.append(len(completed.stdout.splitlines()))
    return QueryRun(True, times, row_counts, hashes, stdout)


def render_query(args: argparse.Namespace, query: str, fusion_enable: str) -> str:
    query = query.strip()
    if not query.endswith(";"):
        query += ";"
    timeout_ms = max(1, int(args.timeout * 1000))
    return f"""
SET search_path TO {args.schema}, public;
SET statement_timeout = {timeout_ms};
SET max_parallel_workers_per_gather = {args.parallel_workers};
SET pg_fusion.enable = {fusion_enable};
{query}
"""


def summarize_query(
    query_id: str,
    pg: QueryRun,
    fusion: QueryRun,
    args: argparse.Namespace,
) -> dict[str, object]:
    result_match = (
        pg.ok
        and fusion.ok
        and outputs_match(pg.stdout, fusion.stdout, args.float_abs_tolerance, args.float_rel_tolerance)
    )
    if not pg.ok:
        status = "pg_fail"
    elif not fusion.ok:
        status = "fusion_fail"
    elif not result_match:
        status = "mismatch"
    else:
        status = "ok"
    ratio = None
    if pg.median_ms and fusion.median_ms:
        ratio = fusion.median_ms / pg.median_ms
    return {
        "query": query_id,
        "status": status,
        "pg_median_ms": pg.median_ms,
        "fusion_median_ms": fusion.median_ms,
        "fusion_vs_pg": ratio,
        "pg_rows": pg.representative_rows,
        "fusion_rows": fusion.representative_rows,
        "result_match": result_match,
        "pg_error": pg.error,
        "fusion_error": fusion.error,
    }


def outputs_match(
    left: str | None,
    right: str | None,
    abs_tolerance: float,
    rel_tolerance: float,
) -> bool:
    if left is None or right is None:
        return False
    if left == right:
        return True
    left_rows = left.splitlines()
    right_rows = right.splitlines()
    if len(left_rows) != len(right_rows):
        return False
    for left_row, right_row in zip(left_rows, right_rows, strict=True):
        left_cells = left_row.split("\t")
        right_cells = right_row.split("\t")
        if len(left_cells) != len(right_cells):
            return False
        for left_cell, right_cell in zip(left_cells, right_cells, strict=True):
            if left_cell == right_cell:
                continue
            if not numeric_cells_match(left_cell, right_cell, abs_tolerance, rel_tolerance):
                return False
    return True


def numeric_cells_match(
    left: str,
    right: str,
    abs_tolerance: float,
    rel_tolerance: float,
) -> bool:
    try:
        left_value = float(left)
        right_value = float(right)
    except ValueError:
        return False
    if not math.isfinite(left_value) or not math.isfinite(right_value):
        return left_value == right_value
    return abs(left_value - right_value) <= max(
        abs_tolerance,
        rel_tolerance * max(abs(left_value), abs(right_value)),
    )


def write_results(
    rows: list[dict[str, object]],
    args: argparse.Namespace,
    results_dir: pathlib.Path,
) -> None:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_path = results_dir / f"tpch_sf_{scale_label(args.scale_factor)}_{stamp}.csv"
    json_path = csv_path.with_suffix(".json")
    headers = [
        "query",
        "status",
        "pg_median_ms",
        "fusion_median_ms",
        "fusion_vs_pg",
        "pg_rows",
        "fusion_rows",
        "result_match",
        "pg_error",
        "fusion_error",
    ]
    with csv_path.open("w", encoding="utf-8") as handle:
        handle.write(",".join(headers) + "\n")
        for row in rows:
            handle.write(",".join(csv_cell(row.get(header)) for header in headers) + "\n")
    json_path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {csv_path}")
    print(f"Wrote {json_path}")


def csv_cell(value: object) -> str:
    if value is None:
        text = ""
    elif isinstance(value, float):
        text = f"{value:.3f}"
    else:
        text = str(value)
    if any(char in text for char in [",", "\n", '"']):
        return '"' + text.replace('"', '""') + '"'
    return text


def print_summary(rows: list[dict[str, object]]) -> None:
    print()
    print("| query | status | pg ms | fusion ms | ratio | rows |")
    print("| --- | --- | ---: | ---: | ---: | ---: |")
    for row in rows:
        print(
            "| {query} | {status} | {pg} | {fusion} | {ratio} | {rows} |".format(
                query=row["query"],
                status=row["status"],
                pg=format_float(row["pg_median_ms"]),
                fusion=format_float(row["fusion_median_ms"]),
                ratio=format_float(row["fusion_vs_pg"]),
                rows=row["fusion_rows"] if row["fusion_rows"] is not None else row["pg_rows"],
            )
        )
    failed = [row for row in rows if row["status"] != "ok"]
    if failed:
        print()
        print("Failures and mismatches:")
        for row in failed:
            detail = row["fusion_error"] or row["pg_error"] or "result mismatch"
            print(f"- {row['query']}: {row['status']}: {detail}")


def format_float(value: object) -> str:
    if value is None:
        return ""
    return f"{float(value):.1f}"


def tail(text: str, max_chars: int = 500) -> str:
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return "..." + text[-max_chars:]


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
