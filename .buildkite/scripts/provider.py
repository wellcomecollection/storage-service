import os


def current_branch():
    return os.environ["BUILDKITE_BRANCH"]


def repo():
    return os.environ["BUILDKITE_REPO"]
