#!/usr/bin/env python3
from pathlib import Path
from typing import List, Sequence
import argparse
import shutil
import subprocess


class YDBProtoFile:
    """
    YDBProtoFile is a proto file lying within YDB directory that
    we have to patch in order to generate valid GRPC for connector.
    """

    src_initial: str
    src_patched: str
    filepath: Path

    def __init__(self, filepath: Path, go_package: str):
        self.filepath = filepath

        # preserve original content
        with open(filepath, "r") as f:
            self.src_initial = f.read()

        # prepare patched version
        lines_initial = self.src_initial.splitlines()

        if "package Ydb;" in lines_initial:
            self.src_patched = self.__patch_ydb_protofile(lines_initial, go_package)
        elif "package Ydb.Issue;" in lines_initial:
            self.src_patched = self.__patch_ydb_protofile(lines_initial, go_package)
        elif "package Ydb.Formats;" in lines_initial:
            self.src_patched = self.__patch_ydb_protofile(lines_initial, go_package)
        elif "package NYql.NConnector.NApi;" in lines_initial:
            self.src_patched = self.__patch_connector_protofile(
                filepath, lines_initial, go_package
            )
        elif "package NYql;" in lines_initial:
            self.src_patched = self.__patch_gateways_config_protofile(
                filepath, lines_initial, go_package
            )
            print(self.src_patched)
        else:
            raise ValueError(f"unknown line pattern for {filepath}")

    def __patch_ydb_protofile(
        self, lines_initial: Sequence[str], go_package: str
    ) -> str:
        import_line_pos = 5
        import_line = f'option go_package = "{go_package}";'

        lines_patched = (
            lines_initial[:import_line_pos]
            + [import_line]
            + lines_initial[import_line_pos:]
        )
        return "\n".join(lines_patched)

    def __patch_connector_protofile(
        self, filepath: Path, lines_initial: Sequence[str], go_package: str
    ) -> str:
        import_line_pos = self.__find_line_by_prefix(
            filepath, lines_initial, "option go_package"
        )
        import_line = f'option go_package = "{go_package}";'

        lines_patched = (
            lines_initial[:import_line_pos]
            + [import_line]
            + lines_initial[import_line_pos + 1 :]
        )
        return "\n".join(lines_patched)

    def __patch_gateways_config_protofile(
        self, filepath: Path, lines_initial: Sequence[str], go_package: str
    ) -> str:
        usefull_start_pos = self.__find_line_by_prefix(
            filepath,
            lines_initial,
            "/////////// Generic gateway for the external data sources ////////////",
        )
        usefull_end_pos = self.__find_line_by_prefix(
            filepath, lines_initial, "message TGenericClusterConfig"
        )

        lines_patched = (
            ['syntax = "proto3";']
            + lines_initial[:1]
            + [f'option go_package = "{go_package}";']
            + lines_initial[usefull_start_pos:usefull_end_pos]
        )

        lines_concatenated = "\n".join(lines_patched)

        # we want protofile to look like it has proto3 syntax
        lines_concatenated = lines_concatenated.replace("optional", "")

        return lines_concatenated

    def __find_line_by_prefix(
        self, filepath: Path, lines_initial: Sequence[str], prefix: str
    ) -> int:
        import_line_pos = None
        for i, line in enumerate(lines_initial):
            if line.startswith(prefix):
                import_line_pos = i
                break

        if not import_line_pos:
            raise ValueError(
                f"unable to find import line in file {filepath}: {lines_initial}"
            )

        return import_line_pos

    def patch(self):
        with open(self.filepath, "w") as f:
            f.write(self.src_patched)

    def revert(self):
        with open(self.filepath, "w") as f:
            f.write(self.src_initial)


# YDB's protofiles this project depends on.
# They will be temporarily patched during the code generation.
source_params = [
    (
        "ydb/public/api/protos/ydb_value.proto",
        "github.com/ydb-platform/ydb-go-genproto/protos/Ydb",
    ),
    (
        "ydb/public/api/protos/ydb_status_codes.proto",
        "github.com/ydb-platform/ydb-go-genproto/protos/Ydb",
    ),
    (
        "ydb/public/api/protos/ydb_formats.proto",
        "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Formats",
    ),
    (
        "ydb/public/api/protos/ydb_issue_message.proto",
        "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue",
    ),
    (
        "yql/essentials/providers/common/proto/gateways_config.proto",
        "github.com/ydb-platform/fq-connector-go/api/common",
    ),
    (
        "ydb/library/yql/providers/generic/connector/api/service/connector.proto",
        "github.com/ydb-platform/fq-connector-go/api/service",
    ),
    (
        "ydb/library/yql/providers/generic/connector/api/service/protos/connector.proto",
        "github.com/ydb-platform/fq-connector-go/api/service/protos",
    ),
    (
        "ydb/library/yql/providers/generic/connector/api/service/protos/error.proto",
        "github.com/ydb-platform/fq-connector-go/api/service/protos",
    ),
]


def __call_subprocess(cmd: List[str]):
    formatted = "\n".join(map(str, cmd))
    print(f"Running command:\n{formatted}")

    process = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = process.communicate()
    exit_code = process.wait()

    if exit_code != 0:
        raise Exception(
            f'Subprocess failure: exit_code={exit_code} stdout={stdout.decode("utf-8")}, stderr={stderr.decode("utf-8")}'
        )

    if stdout:
        print(stdout.decode("utf-8"))
    if stderr:
        print(stderr.decode("utf-8"))
    return stdout


def __find_executable(name: str) -> Path:
    result = shutil.which(name)
    if not result:
        raise ValueError(
            f'executable "{name}" was not found in path, you should install it first'
        )

    return result


def run_protoc(
    proto_files: Sequence[Path],
    target_dir: Path,
    go_module: str,
    includes: Sequence[Path],
    with_grpc: bool,
):
    # compile protoc from Arcadia
    protoc_binary = __find_executable("protoc")
    protoc_gen_go_binary = __find_executable("protoc-gen-go")
    protoc_gen_go_grpc_binary = __find_executable("protoc-gen-go-grpc")

    # build protoc args
    cmd = [
        protoc_binary,
        f"--plugin=protoc-gen-go={protoc_gen_go_binary}",
        f"--plugin=protoc-gen-go-grpc={protoc_gen_go_grpc_binary}",
        f"--go_out={target_dir}",
        f"--go_opt=module={go_module}",
    ]

    if with_grpc:
        cmd.extend(
            [
                f"--go-grpc_out={target_dir}",
                f"--go-grpc_opt=module={go_module}",
            ]
        )

    for include in includes:
        cmd.append(f"-I{include}")

    cmd.extend(proto_files)
    __call_subprocess(cmd)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="generate",
        description="""
        Script for Go Protobuf API generation.
        It takes protofiles from YDB repository and generates Go code in fq-connector-go repository.
        """,
    )

    required_args = parser.add_argument_group("required named arguments")

    required_args.add_argument(
        "--ydb-repo",
        type=str,
        help="Path to the local copy of github.com/ydb-platform/ydb",
        required=True,
    )
    required_args.add_argument(
        "--connector-repo",
        type=str,
        help="Path to the local copy of github.com/ydb-platform/fq-connector-go",
        required=True,
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # derive Arcadia's root
    ydb_github_root = Path(args.ydb_repo)
    if not ydb_github_root.exists():
        raise ValueError(f"path {ydb_github_root} does not exist")

    connector_github_root = Path(args.connector_repo)
    if not connector_github_root.exists():
        raise ValueError(f"path {connector_github_root} does not exist")

    protobuf_includes = ydb_github_root.joinpath(
        "contrib/libs/protobuf/src/google/protobuf"
    )
    if not protobuf_includes.exists():
        raise ValueError(f"path {protobuf_includes} does not exist")

    ydb_source_files = [
        YDBProtoFile(ydb_github_root.joinpath(param[0]), param[1])
        for param in source_params
    ]

    # Patch YDB sources
    for f in ydb_source_files:
        f.patch()
    try:
        # Generate YQL Generic protofiles
        run_protoc(
            [
                ydb_github_root.joinpath(
                    "yql/essentials/providers/common/proto/gateways_config.proto"
                ),
            ],
            connector_github_root.joinpath("api"),
            "github.com/ydb-platform/fq-connector-go/api",
            [ydb_github_root, protobuf_includes],
            True,
        )
        # Generate Connector API
        run_protoc(
            ydb_github_root.joinpath(
                "ydb/library/yql/providers/generic/connector/api"
            ).rglob("*.proto"),
            connector_github_root.joinpath("api"),
            "github.com/ydb-platform/fq-connector-go/api",
            [ydb_github_root, protobuf_includes],
            True,
        )
        # Generate config protofiles
        run_protoc(
            connector_github_root.joinpath("app/config").rglob("*.proto"),
            connector_github_root.joinpath("app/config"),
            "github.com/ydb-platform/fq-connector-go/app/config",
            [ydb_github_root, connector_github_root, protobuf_includes],
            False,
        )
        # Generate split description files
        run_protoc(
            connector_github_root.joinpath("app/server/datasource/rdbms/ydb").rglob(
                "*.proto"
            ),
            connector_github_root.joinpath("app/server/datasource/rdbms/ydb"),
            "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb",
            [connector_github_root, protobuf_includes],
            False,
        )
        run_protoc(
            connector_github_root.joinpath("app/server/datasource/rdbms/postgresql").rglob(
                "*.proto"
            ),
            connector_github_root.joinpath("app/server/datasource/rdbms/postgresql"),
            "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/postgresql",
            [connector_github_root, protobuf_includes],
            False,
        )
        # Generate Cloud Logging protofiles 
        run_protoc(
            connector_github_root.joinpath("app/server/datasource/rdbms/logging").rglob(
                "*.proto"
            ),
            connector_github_root.joinpath("app/server/datasource/rdbms/logging"),
            "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/logging",
            [ydb_github_root, connector_github_root, protobuf_includes],
            False,
        )
        # Generate Connector Observation API
        run_protoc(
            connector_github_root.joinpath(
                "api/observation"
            ).rglob("*.proto"),
            connector_github_root.joinpath("api"),
            "github.com/ydb-platform/fq-connector-go/api",
            [ydb_github_root, connector_github_root, protobuf_includes],
            True,
        )

    finally:
        # Revert changes in YDB sources
        for f in ydb_source_files:
            f.revert()
            pass


if __name__ == "__main__":
    main()
