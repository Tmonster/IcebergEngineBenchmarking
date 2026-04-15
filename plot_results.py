"""
Plot benchmark results from one or more JSON result files.

Usage:
    uv run --extra plot plot_results.py results/a.json results/b.json
    uv run --extra plot plot_results.py results/*.json
    uv run --extra plot plot_results.py results/*.json --output benchmark.png
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def load(paths: list[Path]) -> pd.DataFrame:
    frames = []
    for p in paths:
        rows = json.loads(p.read_text())
        frames.append(pd.DataFrame(rows))
    df = pd.concat(frames, ignore_index=True)
    # Drop errored rows — they have no meaningful elapsed time
    df = df[df["error"].isna()].copy()
    # Label for legend: engine + scale factor
    df["label"] = df["engine"] + " sf" + df["scale_factor"].astype(str)
    return df


def plot(df: pd.DataFrame, output: Path | None) -> None:
    labels = sorted(df["label"].unique())
    queries = sorted(df["query"].unique())

    sns.set_theme(style="whitegrid", font_scale=0.9)
    palette = sns.color_palette("tab10", n_colors=len(labels))

    fig, ax = plt.subplots(figsize=(max(12, len(queries) * 0.8), 6))

    sns.barplot(
        data=df,
        x="query",
        y="elapsed_seconds",
        hue="label",
        hue_order=labels,
        order=queries,
        estimator="median",
        errorbar=("pi", 100),   # min/max whiskers across runs
        palette=palette,
        ax=ax,
    )

    ax.set_title("Query latency by engine (median, min/max across runs)")
    ax.set_xlabel("Query")
    ax.set_ylabel("Elapsed (s)")
    ax.legend(title="Engine", bbox_to_anchor=(1.01, 1), loc="upper left")
    fig.tight_layout()

    if output:
        fig.savefig(output, dpi=150)
        print(f"Saved to {output}")
    else:
        plt.show()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path, help="Result JSON files")
    parser.add_argument("--output", "-o", type=Path, default=None, help="Save to file instead of showing")
    args = parser.parse_args()

    missing = [f for f in args.files if not f.exists()]
    if missing:
        for f in missing:
            print(f"error: file not found: {f}", file=sys.stderr)
        sys.exit(1)

    df = load(args.files)
    if df.empty:
        print("No successful results found in the provided files.", file=sys.stderr)
        sys.exit(1)

    plot(df, args.output)


if __name__ == "__main__":
    main()
