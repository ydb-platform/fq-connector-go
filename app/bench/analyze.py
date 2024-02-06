#!/usr/bin/env python3
import time
import datetime
from pathlib import Path
import json
from dataclasses import dataclass

import matplotlib
import matplotlib.pyplot as plt

import pandas as pd

pd.set_option("display.expand_frame_repr", False)
pd.options.display.float_format = "{:20.2f}".format


@dataclass
class ConnectorBenchmarkReport:
    result_dir: Path  # directory containing reports
    version: str  # semver
    datasource: str  # clickhouse / postgresql
    client: str  # connector / native

    def _read_report(self, filepath: Path) -> pd.Series:
        data = json.load(open(filepath))

        start_time = time.mktime(time.strptime(data["start_time"], "%Y-%m-%d %H:%M:%S"))
        stop_time = time.mktime(time.strptime(data["stop_time"], "%Y-%m-%d %H:%M:%S"))

        return pd.Series(
            {
                "columns": int(len(data["test_case_config"]["columns"])),
                "bytes_internal_rate": data["bytes_internal_rate"],
                "bytes_arrow_rate": data["bytes_arrow_rate"],
                "rows_rate": data["rows_rate"],
                "cpu_utilization": data["cpu_utilization"],
                "latency": datetime.timedelta(seconds=stop_time - start_time).seconds,
            }
        )

    def make_dataframe(self) -> pd.DataFrame:
        series = [self._read_report(path) for path in self.result_dir.glob("*.json")]
        df = pd.DataFrame(series).sort_values("columns")
        df["version"] = self.version
        df["datasource"] = self.datasource
        df["client"] = self.client
        return df


def make_dataframe_from_connector_benchmarks() -> pd.DataFrame:
    reports = [
        ConnectorBenchmarkReport(
            result_dir=Path("/home/vitalyisaev/troubles/YQ-2837/ch_columns_baseline"),
            version="v0.1.1",
            datasource="CH",
            client="connector",
        ),
        ConnectorBenchmarkReport(
            result_dir=Path("/home/vitalyisaev/troubles/YQ-2837/ch_columns_final"),
            version="v0.1.3",
            datasource="CH",
            client="connector",
        ),
        ConnectorBenchmarkReport(
            result_dir=Path("/home/vitalyisaev/troubles/YQ-2837/pg_columns_baseline"),
            version="v0.1.1",
            datasource="PG",
            client="connector",
        ),
        ConnectorBenchmarkReport(
            result_dir=Path("/home/vitalyisaev/troubles/YQ-2837/ch_columns_final"),
            version="v0.1.3",
            datasource="PG",
            client="connector",
        ),
    ]

    df = pd.concat((report.make_dataframe() for report in reports))

    df["columns"] = pd.to_numeric(df["columns"], downcast="integer")

    return df


@dataclass
class NativeBenchmarkReport:
    filepath: Path
    datasource: str  # clickhouse / postgresql
    client: str  # connector / native
    rows_in_table: int  # number of rows

    def make_dataframe(self) -> pd.DataFrame:
        data = json.load(open(self.filepath))
        df = pd.DataFrame(data)
        df["datasource"] = self.datasource
        df["client"] = self.client
        df["rows_rate"] = self.rows_in_table / df["latency"]
        return df


def make_dataframe_from_native_benchmarks() -> pd.DataFrame:
    reports = [
        NativeBenchmarkReport(
            filepath=Path("/home/vitalyisaev/troubles/YQ-2837/ch_native.json"),
            datasource="CH",
            client="clickhouse-client",
            rows_in_table=59986052,
        ),
        NativeBenchmarkReport(
            filepath=Path("/home/vitalyisaev/troubles/YQ-2837/pg_native.json"),
            datasource="PG",
            client="psql",
            rows_in_table=59986052,
        ),
    ]

    df = pd.concat((report.make_dataframe() for report in reports))
    return df


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
    df1 = make_dataframe_from_connector_benchmarks()
    df2 = make_dataframe_from_native_benchmarks()
    df = pd.concat([df1, df2])
    print(df)


if __name__ == "__main__":
    main()
