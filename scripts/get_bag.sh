#!/usr/bin/env bash

BNUM="$(./get_location.sh $1)"
./ss_get_bag.py 'digitised' ${BNUM} 2>/dev/null 1>/dev/null

echo "$1,$BNUM,$?"
