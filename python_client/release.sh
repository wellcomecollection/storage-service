#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

rm -rf dist

python3 setup.py sdist

PYPI_USERNAME="wellcomedigitalplatform"
PYPI_PASSWORD="$(
  AWS_PROFILE=platform-dev \
    aws secretsmanager get-secret-value --secret-id builds/pypi_password | \
    jq -r '.SecretString'
)"

twine upload dist/* --username="$PYPI_USERNAME" --password="$PYPI_PASSWORD"
