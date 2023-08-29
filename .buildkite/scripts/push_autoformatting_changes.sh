#!/usr/bin/env bash
<<EOF
If there are any changes from autoformatting, push them to the PR.

This script is mirrored in our other Scala repos.

== Motivation ==

Running our Scala formatting tool (scalafmt) is moderately slow, enough
so that we don't want to run it as a pre-commit hook.  Instead, we run
it on CI and have it push changes to the pull request branch.

This gives us the benefits of consistently formatted code without having
to pay a multi-second latency penalty on every commit.

EOF

set -o errexit
set -o nounset

# exit-code 1 = changes, exit code 0 = no changes
# See https://git-scm.com/docs/git-diff#Documentation/git-diff.txt---exit-code
git diff --exit-code --quiet && has_changes=$? || has_changes=$?

if (( has_changes == 1 ))
then
  echo "There were changes from formatting, creating a commit"

  git config user.name "Buildkite on behalf of Wellcome Collection"
  git config user.email "wellcomedigitalplatform@wellcome.ac.uk"
  git remote add ssh-origin "$BUILDKITE_REPO" || echo "(remote repo already configured)"

  # We checkout the branch before we add the commit, so we don't
  # include the merge commit that Buildkite makes.
  git fetch ssh-origin

  # If we already have the branch checked out, it's fine.
  git checkout --track "ssh-origin/$BUILDKITE_BRANCH" || echo "(branch already checked out)"

  git add --verbose --update
  git commit -m "Apply auto-formatting rules"
  git push ssh-origin "HEAD:$BUILDKITE_BRANCH"

  # We exit here to fail the build, so Buildkite will skip to the next
  # build, which includes the autoformat commit.
  exit 1
else
  echo "There were no changes from auto-formatting"
fi
