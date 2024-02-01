#!/usr/bin/env python3
from typing import Dict
from pathlib import Path
import json

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd


def read_report(path: Path) -> pd.Series:
    data = json.load(open(path))
    return pd.Series(
        {
            "columns": int(len(data["test_case_config"]["columns"])),
            "bytes_internal_rate": data["bytes_internal_rate"],
            "bytes_arrow_rate": data["bytes_arrow_rate"],
            "rows_rate": data["rows_rate"],
            "cpu_utilization": data["cpu_utilization"],
        }
    )


def make_dataframe(result_dirs: Dict[str, Path]) -> pd.DataFrame:
    dfs = []
    for key, result_dir in result_dirs.items():
        series = [read_report(path) for path in result_dir.glob("*.json")]
        df = pd.DataFrame(series).sort_values("columns")
        df["key"] = key
        dfs.append(df)

    return pd.concat(dfs)


def draw_subplot(
    df_: pd.DataFrame, label: str, y_column: str, ax: matplotlib.figure.Figure
) -> matplotlib.figure.Figure:
    ax.set_ylabel(label)
    ax.set_xlabel("Number of columns to SELECT")

    keys = {
        "baseline": "red",
        "optimized": "blue",
    }

    for key, color in keys.items():
        df = df_.loc[df_["key"] == key]
        ax.plot(df["columns"], df[y_column], color=color, label=key)

    return ax


def draw_plot(df: pd.DataFrame) -> pd.Series:
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(12, 4))
    fig.subplots_adjust(bottom=0.25, wspace=0.5)
    draw_subplot(df, "Throughput, MB/sec", "bytes_internal_rate", axes[0])
    draw_subplot(df, "Throughput, rows/sec", "rows_rate", axes[1])
    ax = draw_subplot(df, "CPU Utilization, %", "cpu_utilization", axes[2])

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower right")
    fig.suptitle("Reading TPC-H S-10 Lineitem from PostgreSQL", fontsize=14)

    fig.savefig("report.png")


def main():
    result_dirs = {
        "baseline": Path(
            "/home/vitalyisaev/projects/fq-connector-go/scripts/bench/postgresql/results/columns_baseline/"
        ),
        "optimized": Path(
            "/home/vitalyisaev/projects/fq-connector-go/scripts/bench/postgresql/results/columns/"
        ),
    }
    df = make_dataframe(result_dirs)
    print(df)
    draw_plot(df)


if __name__ == "__main__":
    main()
