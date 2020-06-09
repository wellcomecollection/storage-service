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

lazy val bag_tagger =
  setupProject(project, "bag_tagger", localDependencies = Seq(common))

lazy val bag_register = setupProject(
  project,
  "bag_register",
  localDependencies = Seq(common, bag_tracker)
)

lazy val bag_replicator =
  setupProject(project, "bag_replicator", localDependencies = Seq(common))

lazy val bag_tracker = setupProject(
  project,
  folder = "bag_tracker",
  localDependencies = Seq(common)
)

lazy val bag_verifier = setupProject(
  project,
  folder = "bag_verifier",
  localDependencies = Seq(common),
  externalDependencies =
    ExternalDependencies.mockitoDependencies
)

lazy val bag_unpacker = setupProject(
  project,
  "bag_unpacker",
  localDependencies = Seq(common),
  externalDependencies =
    ExternalDependencies.commonsCompressDependencies ++
      ExternalDependencies.commonsIODependencies ++
      ExternalDependencies.mockitoDependencies
)

lazy val ingests_tracker = setupProject(
  project,
  "ingests/ingests_tracker",
  localDependencies = Seq(common)
)

lazy val ingests_worker = setupProject(
  project,
  "ingests/ingests_worker",
  localDependencies = Seq(ingests_tracker)
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
  "ingests/ingests_api",
  localDependencies = Seq(display, ingests_tracker),
  externalDependencies = ExternalDependencies.circeOpticsDependencies
)

lazy val bags_api = setupProject(
  project,
  "bags_api",
  localDependencies = Seq(display, bag_tracker),
  externalDependencies = ExternalDependencies.circeOpticsDependencies
)

lazy val indexer_common = setupProject(
  project,
  "indexer/common",
  localDependencies = Seq(common),
  externalDependencies =
    ExternalDependencies.circeOpticsDependencies ++
      ExternalDependencies.elasticsearchDependencies
)

lazy val bag_indexer = setupProject(
  project,
  folder = "indexer/bag_indexer",
  localDependencies = Seq(display, indexer_common, bag_tracker)
)

lazy val ingests_indexer = setupProject(
  project,
  folder = "indexer/ingests_indexer",
  localDependencies = Seq(display, indexer_common)
)
