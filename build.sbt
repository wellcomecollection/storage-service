import java.io.File
import java.util.UUID

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider

def setupProject(
  project: Project,
  folder: String,
  localDependencies: Seq[Project] = Seq(),
  externalDependencies: Seq[ModuleID] = Seq()
): Project = {

  Metadata.write(project, folder, localDependencies)

  val dependsOn = localDependencies
    .map { project: Project =>
      ClasspathDependency(
        project = project,
        configuration = Some("compile->compile;test->test")
      )
    }

  project
    .in(new File(folder))
    .settings(Common.settings: _*)
    .settings(DockerCompose.settings: _*)
    .enablePlugins(DockerComposePlugin)
    .enablePlugins(JavaAppPackaging)
    .dependsOn(dependsOn: _*)
    .settings(libraryDependencies ++= externalDependencies)
}

// Temporarily commented out until https://github.com/wellcometrust/platform/issues/3806
// In order to access our libraries in S3 we need to set the following:

//s3CredentialsProvider := { _ =>
//  val builder = new STSAssumeRoleSessionCredentialsProvider.Builder(
//    "arn:aws:iam::760097843905:role/platform-read_only",
//    UUID.randomUUID().toString
//  )
//
//  builder.build()
//}

lazy val common = setupProject(
  project = project,
  folder = "common",
  externalDependencies = StorageDependencies.commonDependencies
)

lazy val replica_aggregator =
  setupProject(project, "replica_aggregator", localDependencies = Seq(common))

lazy val bag_versioner =
  setupProject(project, "bag_versioner", localDependencies = Seq(common))

lazy val bag_root_finder =
  setupProject(project, "bag_root_finder", localDependencies = Seq(common))

lazy val bag_register = setupProject(
  project,
  "bag_register",
  localDependencies = Seq(common),
  externalDependencies = ExternalDependencies.circeOpticsDependencies
)

lazy val bag_replicator =
  setupProject(project, "bag_replicator", localDependencies = Seq(common))

lazy val bag_verifier =
  setupProject(project, "bag_verifier", localDependencies = Seq(common))

lazy val bag_unpacker = setupProject(
  project,
  "bag_unpacker",
  localDependencies = Seq(common),
  externalDependencies =
    ExternalDependencies.commonsCompressDependencies ++
      ExternalDependencies.commonsIODependencies ++
      ExternalDependencies.mockitoDependencies
)

lazy val ingests_common =
  setupProject(project, "ingests_common", localDependencies = Seq(common))

lazy val ingests = setupProject(
  project,
  "ingests",
  localDependencies = Seq(ingests_common),
  externalDependencies = ExternalDependencies.wiremockDependencies
)

lazy val display =
  setupProject(project, "display", localDependencies = Seq(common))

lazy val notifier = setupProject(
  project,
  "notifier",
  localDependencies = Seq(display),
  externalDependencies = ExternalDependencies.wiremockDependencies ++ ExternalDependencies.circeOpticsDependencies
)

lazy val ingests_api = setupProject(
  project,
  "api/ingests_api",
  localDependencies = Seq(ingests_common, display),
  externalDependencies = ExternalDependencies.circeOpticsDependencies
)

lazy val bags_api = setupProject(
  project,
  "api/bags_api",
  localDependencies = Seq(display),
  externalDependencies = ExternalDependencies.circeOpticsDependencies
)

lazy val indexer_common = setupProject(
  project,
  "indexer/common",
  externalDependencies =
    ExternalDependencies.elasticsearchDependencies ++
      WellcomeDependencies.fixturesLibrary ++
      WellcomeDependencies.jsonLibrary ++
      WellcomeDependencies.typesafeLibrary
)

lazy val ingests_indexer = setupProject(
  project,
  folder = "indexer/ingests_indexer",
  localDependencies = Seq(display, indexer_common)
)
