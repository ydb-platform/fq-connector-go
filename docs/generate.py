#!/usr/bin/env python3

import os
import sys

import pandas as pd


def make_table() -> str:
    df = pd.read_csv("types.csv")
    out = df.to_markdown(index=False)
    print(out)
    return out


def read_template() -> str:
    with open("type_mapping_table.md.template", "r") as f:
        return f.read()


def make_page():
    out = read_template() + make_table()
    with open("type_mapping_table.md", "w") as f:
        f.write(out)


def main():
    if len(sys.argv) != 2:
        print("unexpected number of elements")
        exit(1)

    os.chdir(sys.argv[1])
    make_page()


if __name__ == "__main__":
    main()
