#!/usr/bin/env bash

./ss_get_ingest.py $1 | sed -n 2p | awk '{print $2}' | awk -F'/' '{print $4}' | awk -F'.' '{print $1}'
