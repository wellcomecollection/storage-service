import java.io.File
import java.util.UUID

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider

def setupProject(
  project: Project,
  folder: String,
  localDependencies: Seq[Project] = Seq(),
  externalDependencies: Seq[ModuleID] = Seq(),
  description: String
): Project = {

  Metadata.write(project, folder, localDependencies, description)

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
    .enablePlugins(DockerComposePlugin)
    .enablePlugins(JavaAppPackaging)
    .dependsOn(dependsOn: _*)
    .settings(libraryDependencies ++= externalDependencies)
}

lazy val common = setupProject(
  project = project,
  folder = "common",
  externalDependencies = StorageDependencies.commonDependencies,
  description = "Common utilities shared throughout the storage-service"
)

lazy val replica_aggregator = setupProject(
  project,
  folder = "replica_aggregator",
  localDependencies = Seq(common),
  description =
    "Checks a bag has enough successful replicas before allowing it to continue"
)

lazy val bag_versioner = setupProject(
  project,
  folder = "bag_versioner",
  localDependencies = Seq(common),
  description = "Assigns a version to a bag as it's being ingested"
)

lazy val bag_root_finder = setupProject(
  project,
  folder = "bag_root_finder",
  localDependencies = Seq(common),
  description =
    "Finds the root of an unpacked bag (was it compressed top-level or in a directory?)"
)

lazy val bag_register = setupProject(
  project,
  folder = "bag_register",
  localDependencies = Seq(common, bag_tracker),
  description =
    "Creates a storage manifest for a bag once ingested, so the bag can be retrieved from the bags API"
)

lazy val bag_replicator = setupProject(
  project,
  folder = "bag_replicator",
  localDependencies = Seq(common),
  externalDependencies = ExternalDependencies.mockitoDependencies,
  description = "Replicates the bag to one or more permanent storage locations"
)

lazy val bag_tracker = setupProject(
  project,
  folder = "bag_tracker",
  localDependencies = Seq(common),
  description =
    "Internal app: controls access to the database of storage manifests (i.e. metadata descriptions of bags)"
)

lazy val bag_tagger = setupProject(
  project,
  folder = "bag_tagger",
  localDependencies = Seq(bag_tracker),
  description =
    "Adds tags to bags in permanent storage after they've been successfully stored"
)

lazy val bag_verifier = setupProject(
  project,
  folder = "bag_verifier",
  localDependencies = Seq(common),
  externalDependencies = ExternalDependencies.mockitoDependencies,
  description =
    "Runs checks and rules against a bag: fixity information, file sizes, no missing files, and so on"
)

lazy val bag_unpacker = setupProject(
  project,
  "bag_unpacker",
  localDependencies = Seq(common),
  externalDependencies =
    ExternalDependencies.commonsCompressDependencies ++
      ExternalDependencies.commonsIODependencies,
  description =
    "Unpacks a bag that's compressed as a tar.gz into individual files"
)

lazy val ingests_tracker = setupProject(
  project,
  folder = "ingests/ingests_tracker",
  localDependencies = Seq(common),
  description = "Internal app: controls access to the ingests database"
)

lazy val ingests_worker = setupProject(
  project,
  folder = "ingests/ingests_worker",
  localDependencies = Seq(ingests_tracker),
  description = "Records ingest updates sent by other services in the pipeline"
)

lazy val display = setupProject(
  project,
  folder = "display",
  localDependencies = Seq(common),
  description = "Models used in the user-facing APIs (ingests and bags APIs)"
)

lazy val notifier = setupProject(
  project,
  folder = "notifier",
  localDependencies = Seq(display),
  description =
    "Sends a callback notification to an external service after an ingest completes"
)

lazy val ingests_api = setupProject(
  project,
  folder = "ingests/ingests_api",
  localDependencies = Seq(display, ingests_tracker),
  externalDependencies = ExternalDependencies.circeOpticsDependencies,
  description = "User-facing ingests API"
)

lazy val bags_api = setupProject(
  project,
  folder = "bags_api",
  localDependencies = Seq(display, bag_tracker),
  externalDependencies = ExternalDependencies.circeOpticsDependencies,
  description = "User-facing bags API"
)

lazy val indexer_common = setupProject(
  project,
  folder = "indexer/common",
  localDependencies = Seq(common),
  externalDependencies =
    ExternalDependencies.circeOpticsDependencies ++
      WellcomeDependencies.elasticsearchLibrary ++
      WellcomeDependencies.elasticsearchTypesafeLibrary,
  description = "Common utilities shared among the indexer applications"
)

lazy val bag_indexer = setupProject(
  project,
  folder = "indexer/bag_indexer",
  localDependencies = Seq(display, indexer_common, bag_tracker),
  description =
    "Indexes top-level information about bags in an Elasticsearch cluster"
)

lazy val file_finder = setupProject(
  project,
  folder = "indexer/file_finder",
  localDependencies = Seq(bag_tracker, indexer_common),
  description =
    "Finds new files that need indexing after a bag is successfully stored"
)

lazy val file_indexer = setupProject(
  project,
  folder = "indexer/file_indexer",
  localDependencies = Seq(indexer_common),
  description = "Indexes information about files in an Elasticsearch cluster"
)

lazy val ingests_indexer = setupProject(
  project,
  folder = "indexer/ingests_indexer",
  localDependencies = Seq(display, indexer_common),
  description = "Indexes information about ingests in an Elasticsearch cluster"
)

// AWS Credentials to read from S3

s3CredentialsProvider := { _ =>
  val builder = new STSAssumeRoleSessionCredentialsProvider.Builder(
    "arn:aws:iam::760097843905:role/platform-read_only",
    UUID.randomUUID().toString
  )
  builder.build()
}
