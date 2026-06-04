from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from classifier_perf_utils import (
        DEFAULT_RULES_PATH,
        DEFAULT_WORKDIR,
        audit_rules,
        classifier_sizes_for_args,
        compare_classified_outputs,
        ensure_mock_classifier_files,
        find_real_processed_sample,
        render_comparison_text,
        render_result_text,
        render_rules_audit_text,
        run_instrumented_classification,
        write_json,
        write_real_baseline_sample,
        write_real_sample,
        write_text,
    )
except ModuleNotFoundError:
    from scripts.classifier_perf_utils import (
        DEFAULT_RULES_PATH,
        DEFAULT_WORKDIR,
        audit_rules,
        classifier_sizes_for_args,
        compare_classified_outputs,
        ensure_mock_classifier_files,
        find_real_processed_sample,
        render_comparison_text,
        render_result_text,
        render_rules_audit_text,
        run_instrumented_classification,
        write_json,
        write_real_baseline_sample,
        write_real_sample,
        write_text,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark and audit the current CSV classifier.")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run mock and optional real-data classifier benchmarks.")
    run_parser.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR, help="Directory for generated data and outputs.")
    run_parser.add_argument("--rules-path", type=Path, default=DEFAULT_RULES_PATH, help="Current real classifier rules.")
    run_parser.add_argument("--size", choices=("small", "medium", "large"), default="small", help="Mock dataset size.")
    run_parser.add_argument("--all-sizes", action="store_true", help="Run all non-large mock sizes by default.")
    run_parser.add_argument("--include-large", action="store_true", help="Allow the large 500k-row benchmark.")
    run_parser.add_argument("--skip-mock", action="store_true", help="Skip mock benchmark.")
    run_parser.add_argument("--skip-real", action="store_true", help="Skip real small sample benchmark.")
    run_parser.add_argument("--real-rows", type=int, default=10_000, help="Rows to copy from the detected real processed CSV.")

    audit_parser = subparsers.add_parser("audit-rules", help="Audit classifier rules without running benchmarks.")
    audit_parser.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR)
    audit_parser.add_argument("--rules-path", type=Path, default=DEFAULT_RULES_PATH)
    audit_parser.add_argument("--sample-input", type=Path, help="Optional sample CSV/XLSX for match-rate audit.")
    audit_parser.add_argument("--sample-limit", type=int, default=10_000)

    compare_parser = subparsers.add_parser("compare", help="Compare baseline and candidate classified CSV outputs.")
    compare_parser.add_argument("baseline", type=Path)
    compare_parser.add_argument("candidate", type=Path)
    compare_parser.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR)
    compare_parser.add_argument("--columns", nargs="*", help="Classification columns to compare.")
    compare_parser.add_argument("--max-differences", type=int, default=20)

    return parser.parse_args()


def _run_benchmarks(args: argparse.Namespace) -> None:
    args.workdir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []

    if not args.skip_mock:
        for size in classifier_sizes_for_args(size=args.size, all_sizes=args.all_sizes, include_large=args.include_large):
            input_path, rules_path, _ = ensure_mock_classifier_files(workdir=args.workdir, size=size)
            result = run_instrumented_classification(
                name=f"mock_{size}",
                input_path=input_path,
                output_path=args.workdir / "mock" / f"benchmark_{size}_classified.csv",
                rules_path=rules_path,
                enable_cprofile=False,
            )
            write_json(args.workdir / f"benchmark_mock_{size}.json", result)
            write_text(args.workdir / f"benchmark_mock_{size}.txt", render_result_text(result))
            results.append(result)

    real_sample = None
    if not args.skip_real:
        source = find_real_processed_sample()
        if source is not None:
            real_dir = args.workdir / "real"
            real_dir.mkdir(parents=True, exist_ok=True)
            real_sample = write_real_sample(source, real_dir / f"real_sample_{args.real_rows}.csv", rows=args.real_rows)
            baseline = write_real_baseline_sample(source, real_dir / f"real_sample_{args.real_rows}_baseline.csv", rows=args.real_rows)
            result = run_instrumented_classification(
                name=f"real_{args.real_rows}",
                input_path=real_sample,
                output_path=real_dir / f"real_sample_{args.real_rows}_classified.csv",
                rules_path=args.rules_path,
                enable_cprofile=False,
            )
            if baseline is not None:
                result["comparison"] = compare_classified_outputs(
                    baseline,
                    Path(str(result["output_file"])),
                    max_differences=20,
                )
            write_json(args.workdir / f"benchmark_real_{args.real_rows}.json", result)
            write_text(args.workdir / f"benchmark_real_{args.real_rows}.txt", render_result_text(result))
            results.append(result)

    rules_audit = audit_rules(args.rules_path, sample_input=real_sample, sample_limit=args.real_rows)
    write_json(args.workdir / "rules_audit.json", rules_audit)
    write_text(args.workdir / "rules_audit.txt", render_rules_audit_text(rules_audit))

    summary = {
        "results": results,
        "rules_audit": {
            key: value
            for key, value in rules_audit.items()
            if key not in {"sample_report"}
        },
    }
    write_json(args.workdir / "benchmark_summary.json", summary)
    lines = ["# Classifier Benchmark Summary", ""]
    for result in results:
        lines.append(
            f"- {result['name']}: rows={result['rows_output']}, rules={result['active_rules']}, "
            f"duration={result['total_seconds']:.4f}s, rows/sec={result['rows_per_second']:.2f}, "
            f"top={result['top_bottleneck']}"
        )
    lines.extend(["", "## Rules Audit", ""])
    lines.append(render_rules_audit_text(rules_audit).strip())
    write_text(args.workdir / "benchmark_summary.txt", "\n".join(lines) + "\n")

    for result in results:
        print(
            f"{result['name']}: {result['rows_output']} rows, "
            f"{result['total_seconds']:.3f}s, {result['rows_per_second']:.1f} rows/sec, "
            f"top={result['top_bottleneck']}"
        )
    print(f"rules suspicious: {len(rules_audit['suspicious_rules'])}")


def _run_audit_rules(args: argparse.Namespace) -> None:
    audit = audit_rules(args.rules_path, sample_input=args.sample_input, sample_limit=args.sample_limit)
    write_json(args.workdir / "rules_audit.json", audit)
    write_text(args.workdir / "rules_audit.txt", render_rules_audit_text(audit))
    print(render_rules_audit_text(audit).strip())


def _run_compare(args: argparse.Namespace) -> None:
    comparison = compare_classified_outputs(
        args.baseline,
        args.candidate,
        columns=args.columns,
        max_differences=args.max_differences,
    )
    write_json(args.workdir / "classifier_comparison.json", comparison)
    write_text(args.workdir / "classifier_comparison.txt", render_comparison_text(comparison) + "\n")
    print(render_comparison_text(comparison))


def main() -> None:
    args = parse_args()
    if args.command is None:
        args.command = "run"
        args.workdir = DEFAULT_WORKDIR
        args.rules_path = DEFAULT_RULES_PATH
        args.size = "small"
        args.all_sizes = False
        args.include_large = False
        args.skip_mock = False
        args.skip_real = False
        args.real_rows = 10_000
    command = args.command
    if command == "run":
        _run_benchmarks(args)
        return
    if command == "audit-rules":
        _run_audit_rules(args)
        return
    if command == "compare":
        _run_compare(args)
        return
    raise ValueError(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
