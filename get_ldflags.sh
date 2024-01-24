#!/bin/bash

echo "-X 'github.com/ydb-platform/fq-connector-go/app/version.InfoSource=git' \
      -X 'github.com/ydb-platform/fq-connector-go/app/version.Branch=$(git rev-parse --abbrev-ref HEAD)' \
      -X 'github.com/ydb-platform/fq-connector-go/app/version.CommitHash=$(git rev-parse HEAD)' \
      -X 'github.com/ydb-platform/fq-connector-go/app/version.Tag=$(git describe --tags)' \
      -X 'github.com/ydb-platform/fq-connector-go/app/version.Author=$(git log -1 --pretty=format:'%an')' \
      -X 'github.com/ydb-platform/fq-connector-go/app/version.CommitDate=$(git show -s --format=%cd --date=format:'%Y-%m-%d %H:%M:%S')' \
      -X 'github.com/ydb-platform/fq-connector-go/app/version.CommitMessage=$(git log -1 --pretty=%B)'
      -X 'github.com/ydb-platform/fq-connector-go/app/version.Username=$(echo $USER)'
      -X 'github.com/ydb-platform/fq-connector-go/app/version.BuildLocation=$(pwd)'
      -X 'github.com/ydb-platform/fq-connector-go/app/version.Hostname=$(hostname)'
      -X 'github.com/ydb-platform/fq-connector-go/app/version.HostInfo=$(uname -s) $(hostname) $(uname -r) $(date) $(uname -m)'
      -X 'github.com/ydb-platform/fq-connector-go/app/version.PathToGo=$(which go)'
      -X 'github.com/ydb-platform/fq-connector-go/app/version.GoVersion=$(go version)'"

