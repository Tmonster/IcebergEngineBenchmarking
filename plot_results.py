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


def load(paths: list[Path]) -> tuple[pd.DataFrame, dict[str, float]]:
    frames = []
    for p in paths:
        rows = json.loads(p.read_text())
        frames.append(pd.DataFrame(rows))
    df = pd.concat(frames, ignore_index=True)
    df["label"] = df["engine"] + " sf" + df["scale_factor"].astype(str)

    # Extract power scores before filtering (power_score rows have elapsed_seconds=None)
    power_scores: dict[str, float] = {}
    if "power_score" in df.columns:
        score_rows = df[df["query"] == "power_score"]
        for _, row in score_rows.iterrows():
            if pd.notna(row.get("power_score")):
                power_scores[row["label"]] = row["power_score"]

    # Keep only rows with a valid elapsed time and no error
    df = df[(df["query"] != "power_score") & df["error"].isna() & df["elapsed_seconds"].notna()].copy()
    return df, power_scores


def plot(df: pd.DataFrame, power_scores: dict[str, float], output: Path | None) -> None:
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

    title = "Query latency by engine (median, min/max across runs)"
    if power_scores:
        score_parts = [f"{label}: {score:,.2f} QphH" for label, score in sorted(power_scores.items())]
        title += "\nPower score — " + " | ".join(score_parts)

    ax.set_title(title)
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

    df, power_scores = load(args.files)
    if df.empty:
        print("No successful results found in the provided files.", file=sys.stderr)
        sys.exit(1)

    plot(df, power_scores, args.output)


if __name__ == "__main__":
    main()
