# -*- encoding: utf-8

from commands import git


def get_changed_paths(*args, globs=None):
    """
    Returns a set of changed paths in a given commit range.

    :param args: Arguments to pass to ``git diff``.
    :param globs: List of file globs to include in changed paths.
    """
    if globs:
        args = list(args) + ["--", *globs]
    diff_output = git("diff", "--name-only", *args)

    return {line.strip() for line in diff_output.splitlines()}


def remote_default_branch():
    """Inspect refs to discover default branch @ remote origin."""
    return git("symbolic-ref", "refs/remotes/origin/HEAD").split("/")[-1]


def remote_default_head():
    """Inspect refs to discover default branch HEAD @ remote origin."""
    return git("show-ref", f"refs/remotes/origin/{remote_default_branch()}", "-s")


def local_current_head():
    """Use rev-parse to discover hash for current commit AKA HEAD (from .git/HEAD)."""
    return git("rev-parse", "HEAD")


def get_sha1_for_tag(tag):
    """Use show-ref to discover the hash for a given tag (fetch first so we have all remote tags)."""
    git("fetch")
    return git("show-ref", "--hash", tag)
