# How Docker images are published to ECR

We run our services in Docker containers.
This page explains how a merge to the `main` branch becomes a Docker image in ECR that we can use.

1.	Somebody merges a pull request to `main`.

2.	This merge commit is picked up by BuildKite, our CI runner.

    Our BuildKite instance runs on EC2 instances in our AWS account, and the logs are only visible to Wellcome Collection developers.

3.  BuildKite looks at each project in turn, and decides if it has any changes that need rebuilding.
    Changes that might trigger a rebuild include:

    -   Changes to the source code of an application
    -   Changes to the source code of a library that an app depends on
    -   Changes to `Dependencies.scala`, which might mean a new version of an underlying library

    If there are no changes, BuildKite exists early.

4.  If there are changes, BuildKite runs any project tests and then builds a Docker image using the `Dockerfile` in the project directory.

    ![Screenshot of BuildKite. There's a large green box titled 'Merge pull request #895 from wellcomecollection/windows-line-endings' with some links to the branch/commit below it. Inside the box are smaller boxes named for each project arranged over a couple of lines: common, display, bags API, and so on.](buildkite-main-build.png)

    The image is tagged with the Git ref of the merge commit (e.g. `ref.9ff8df730259938685785337579137e097a9157f`) and the tag `latest`.
    Note that the `latest` tag is a tag that moves to reflect the last image pushed by BuildKite.

    BuildKite then publishes the Docker image to two locations: ECR and ECR Public.
