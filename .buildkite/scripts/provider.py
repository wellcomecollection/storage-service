# -*- encoding: utf-8

import os

from git_utils import remote_default_branch


def current_branch():
    return os.environ["BUILDKITE_BRANCH"]


def is_default_branch():
    current_branch_name = current_branch()
    default_branch_name = remote_default_branch()

    return current_branch_name == default_branch_name


def repo():
    return os.environ["BUILDKITE_REPO"]
