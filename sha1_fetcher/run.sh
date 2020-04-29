#!/usr/bin/env bash

set -o errexit
set -o nounset

python3 /runner.py &
python3 /runner.py &
python3 /runner.py
