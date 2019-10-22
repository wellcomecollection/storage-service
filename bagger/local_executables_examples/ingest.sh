#!/usr/bin/env bash

set -o nounset
set -o verbose

export AWS_PROFILE=storage

xargs -I '{}' ./migtool.sh ingest --delay 0 --filter '{}/' < bnumber_prefixes.txt