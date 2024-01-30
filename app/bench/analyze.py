#!/usr/bin/env python3

from pathlib import Path
import json

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
        }
    )


def make_dataframe(result_dir: Path) -> pd.DataFrame:
    series = [read_report(path) for path in result_dir.glob("*.json")]
    return pd.DataFrame(series)


def draw_plot(df: pd.DataFrame, result_dir: Path) -> pd.Series:
    fig, ax1 = plt.subplots()

    ax1.set_xlabel("Number of columns in SELECT")
    ax1.set_ylabel("Throughput, MB/sec", color="red")
    ax1.scatter(df["columns"], df["bytes_internal_rate"], color="red")
    ax1.tick_params(axis="y", labelcolor="red")

    ax2 = ax1.twinx()

    ax2.set_ylabel("Throughput, rows/sec", color="blue")
    ax2.scatter(df["columns"], df["rows_rate"], color="blue")
    ax2.tick_params(axis="y", labelcolor="blue")

    fig.savefig(result_dir.joinpath("report.png"))


def main():
    result_dir = Path(
        "/home/vitalyisaev/projects/fq-connector-go/scripts/bench/postgresql/results/columns/"
    )
    df = make_dataframe(result_dir)
    print(df)
    draw_plot(df, result_dir)


if __name__ == "__main__":
    main()
