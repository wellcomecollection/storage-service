#!/usr/bin/env python
# -*- encoding: utf-8

import pytest

from run_job import should_run_sbt_project
from sbt_dependency_tree import Repository


@pytest.fixture(scope="session")
def repo():
    yield Repository(".sbt_metadata")


@pytest.mark.parametrize(
    "project_name, changed_paths, should_run_project",
    [
        ("id_minter", ["snapshots/Makefile"], False),
        ("id_minter", ["pipeline/Makefile"], True),
        ("id_minter", ["common/Makefile", "pipeline/Makefile"], True),
        ("elasticsearch", ["common/Makefile"], True),
        ("elasticsearch", ["common/Makefile", "pipeline/Makefile"], True),
        ("elasticsearch", ["common/Makefile", "pipeline/Makefile"], True),
        ("big_messaging_typesafe", ["common/big_messaging/file.scala"], False),
        ("merger", ["common/big_messaging/file.scala"], False),
        ("merger", ["common/big_messaging_typesafe/file.scala"], True),
    ],
)
def test_should_run_sbt_project(repo, project_name, changed_paths, should_run_project):
    result = should_run_sbt_project(repo, project_name, changed_paths=changed_paths)
    assert result == should_run_project
