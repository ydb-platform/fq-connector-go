#!/usr/bin/env python3
import time
import datetime
from pathlib import Path
import json
from dataclasses import dataclass

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

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
        df["datasource"] = self.datasource
        df["client"] = f"{self.client}\n{self.version}"
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
            result_dir=Path("/home/vitalyisaev/troubles/YQ-2837/pg_columns_final"),
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
            client="clickhouse\nclient",
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


@ticker.FuncFormatter
def million_frac_formatter(x, pos):
    return "%.1f M" % (x / 1e6)


@ticker.FuncFormatter
def million_int_formatter(x, pos):
    return "%d M" % (x / 1e6)


def draw_columnar_subplot_datasource(
    ax: matplotlib.figure.Figure,
    df: pd.DataFrame,
    datasource_df: pd.DataFrame,
    label: str,
    y_column: str,
) -> matplotlib.figure.Figure:
    ax.set_ylabel(label)
    ax.set_xlabel("Number of columns to SELECT")

    if datasource_df[y_column].max() > 1000000:
        ax.yaxis.set_major_formatter(million_int_formatter)

    ax.set_ylim([df[y_column].min(), df[y_column].max()])

    for client in pd.unique(datasource_df["client"]):
        client_df = datasource_df[datasource_df["client"] == client]
        ax.plot(client_df["columns"], client_df[y_column], label=client)

    return ax


def draw_columnar_plot_datasource(df: pd.DataFrame, datasource: str):
    datasource_df = df[df["datasource"] == datasource]
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(12, 4))
    fig.subplots_adjust(bottom=0.25, wspace=0.5)
    draw_columnar_subplot_datasource(
        axes[0], df, datasource_df, "Throughput, MB/sec", "bytes_internal_rate"
    )
    draw_columnar_subplot_datasource(
        axes[1], df, datasource_df, "Throughput, rows/sec", "rows_rate"
    )
    ax = draw_columnar_subplot_datasource(
        axes[2], df, datasource_df, "CPU Utilization, %", "cpu_utilization"
    )

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower right")
    fig.suptitle(f"Reading TPC-H S-10 Lineitem from {datasource}", fontsize=14)
    fig.savefig(f"plot_columnar_{datasource}.png")


def draw_columnar_plot(df: pd.DataFrame):
    for datasource in ["CH", "PG"]:
        draw_columnar_plot_datasource(df, datasource)


def draw_overall_plot(src: pd.DataFrame):
    # max table width
    df = src[src["columns"] == src["columns"].max()]

    fig, axes = plt.subplots(ncols=2, nrows=1, sharey=True)
    fig.suptitle("Reading TPC-H S-10 Lineitem table", fontsize=14)

    once = False

    for i, datasource in enumerate(pd.unique(df["datasource"])):
        datasource_df = df[df["datasource"] == datasource]

        colors = [
            "blue" if "connector" in client else "red"
            for client in datasource_df["client"]
        ]

        ax = axes[i]
        ax.bar(
            datasource_df["client"],
            datasource_df["rows_rate"],
            align="center",
            color=colors,
            width=0.3,
        )

        if not once:
            ax.set_ylabel("Throughput, rows/sec")
            ax.yaxis.set_major_formatter(million_frac_formatter)
            once = True

        ax.set_title(datasource)

    fig.savefig("plot_overall.png")


def make_overall_dataframe() -> pd.DataFrame:
    df1 = make_dataframe_from_connector_benchmarks()
    df2 = make_dataframe_from_native_benchmarks()
    df = pd.concat([df1, df2])
    return df


def main():
    df = make_overall_dataframe()
    print(df)
    draw_overall_plot(df)
    draw_columnar_plot(df)


if __name__ == "__main__":
    main()
