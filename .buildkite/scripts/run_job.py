#!/usr/bin/env python3
# -*- encoding: utf-8

import argparse
import os
import sys

from commands import make
from git_utils import (
    local_current_head,
    get_sha1_for_tag,
    remote_default_head,
    get_changed_paths,
)
from provider import current_branch, is_default_branch
from sbt_dependency_tree import Repository


def should_run_sbt_project(repo, project_name, changed_paths):
    project = repo.get_project(project_name)

    interesting_paths = [p for p in changed_paths if not p.startswith(".sbt_metadata")]

    if ".buildkite/pipeline.yml" in interesting_paths:
        print("*** Relevant: .buildkite/pipeline.yml")
        return True
    if "build.sbt" in interesting_paths:
        print("*** Relevant: build.sbt")
        return True
    if any(p.startswith("project/") for p in interesting_paths):
        print("*** Relevant: project/")
        return True

    for path in interesting_paths:
        if path.startswith((".buildkite", "docs/")):
            continue

        if path.endswith((".py", ".tf", ".md")):
            continue

        if path.endswith("Makefile"):
            if os.path.dirname(project.folder) == os.path.dirname(path):
                print("*** %s is defined by %s" % (project.name, path))
                return True
            else:
                continue

        try:
            project_for_path = repo.lookup_path(path)
        except KeyError:
            # This path isn't associated with a project!
            print("*** Unrecognised path: %s" % path)
            return True
        else:
            if project.depends_on(project_for_path):
                print("*** %s depends on %s" % (project.name, project_for_path.name))
                return True

        print("*** Not significant: %s" % path)

    return False


if __name__ == "__main__":
    # Get git metadata

    commit_range = None
    local_head = local_current_head()

    if is_default_branch():
        latest_sha = get_sha1_for_tag("latest")
        commit_range = f"{latest_sha}..{local_head}"
    else:
        remote_head = remote_default_head()
        commit_range = f"{remote_head}..{local_head}"

    print(f"Working in branch: {current_branch()}")
    print(f"On default branch: {is_default_branch()}")
    print(f"Commit range: {commit_range}")

    # Parse script args

    parser = argparse.ArgumentParser()
    parser.add_argument("project_name", default=os.environ.get("SBT_PROJECT"))
    parser.add_argument("--changes-in", nargs="*")
    args = parser.parse_args()

    # Get changed_paths

    if args.changes_in:
        change_globs = args.changes_in + [".buildkite/pipeline.yml"]
    else:
        change_globs = None

    changed_paths = get_changed_paths(commit_range, globs=change_globs)

    # Determine whether we should build this project

    sbt_repo = Repository(".sbt_metadata")
    try:
        if not should_run_sbt_project(sbt_repo, args.project_name, changed_paths):
            print(f"Nothing in this patch affects {args.project_name}, so stopping.")
            sys.exit(0)
    except (FileNotFoundError, KeyError):
        if args.changes_in and not changed_paths:
            print(
                f"Nothing in this patch affects the files {args.changes_in}, so stopping."
            )
            sys.exit(0)

    # Perform make tasks

    make(f"{args.project_name}-test")

    if is_default_branch():
        make(f"{args.project_name}-publish")
