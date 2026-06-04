from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from classifier_perf_utils import (
        DEFAULT_WORKDIR,
        classifier_sizes_for_args,
        ensure_mock_classifier_files,
        render_result_text,
        run_instrumented_classification,
        write_json,
        write_text,
    )
except ModuleNotFoundError:
    from scripts.classifier_perf_utils import (
        DEFAULT_WORKDIR,
        classifier_sizes_for_args,
        ensure_mock_classifier_files,
        render_result_text,
        run_instrumented_classification,
        write_json,
        write_text,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile the current CSV classifier with cProfile and phase timings."
    )
    parser.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR, help="Directory for generated data and profile outputs.")
    parser.add_argument("--size", choices=("small", "medium", "large"), default="small", help="Mock dataset size.")
    parser.add_argument("--all-sizes", action="store_true", help="Run all non-large sizes by default.")
    parser.add_argument("--include-large", action="store_true", help="Allow the large 500k-row profile.")
    parser.add_argument("--input", type=Path, help="Optional existing CSV/XLSX input. Uses --rules-path and writes profile_custom.*.")
    parser.add_argument("--rules-path", type=Path, help="Rules CSV/XLSX for --input. Mock rules are used otherwise.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, object]] = []
    if args.input:
        if not args.rules_path:
            raise ValueError("--rules-path is required with --input")
        result = run_instrumented_classification(
            name="custom",
            input_path=args.input,
            output_path=args.workdir / "profile_custom_output.csv",
            rules_path=args.rules_path,
            enable_cprofile=True,
        )
        write_json(args.workdir / "profile_custom.json", result)
        write_text(args.workdir / "profile_custom.txt", render_result_text(result))
        results.append(result)
    else:
        for size in classifier_sizes_for_args(size=args.size, all_sizes=args.all_sizes, include_large=args.include_large):
            input_path, rules_path, _ = ensure_mock_classifier_files(workdir=args.workdir, size=size)
            result = run_instrumented_classification(
                name=size,
                input_path=input_path,
                output_path=args.workdir / "mock" / f"profile_{size}_classified.csv",
                rules_path=rules_path,
                enable_cprofile=True,
            )
            write_json(args.workdir / f"profile_{size}.json", result)
            write_text(args.workdir / f"profile_{size}.txt", render_result_text(result))
            results.append(result)

    for result in results:
        print(
            f"{result['name']}: {result['rows_output']} rows, "
            f"{result['total_seconds']:.3f}s, {result['rows_per_second']:.1f} rows/sec, "
            f"top={result['top_bottleneck']}"
        )


if __name__ == "__main__":
    main()
