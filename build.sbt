import java.io.File

import scala.util.parsing.json.{JSONArray, JSONObject}

def setupProject(
  project: Project,
  folder: String,
  localDependencies: Seq[Project] = Seq(),
  externalDependencies: Seq[ModuleID] = Seq()
): Project = {

  // Here we write a bit of metadata about the project, and the other
  // local projects it depends on.  This can be used to determine whether
  // to run tests based on the up-to-date project graph.
  // See https://www.scala-sbt.org/release/docs/Howto-Generating-Files.html
  val file = new File(s".sbt_metadata/${project.id}.json")
  val dependencyIds: List[String] = localDependencies
    .map { p: Project => p.id }
    .toList

  val metadata = Map(
    "id" -> project.id,
    "folder" -> folder,
    "dependencyIds" -> JSONArray(dependencyIds)
  )

  IO.write(file, JSONObject(metadata).toString())

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

lazy val common = setupProject(project, "common",
  externalDependencies = StorageDependencies.commonDependencies
)

lazy val display = setupProject(project, "display",
  localDependencies = Seq(common)
)

lazy val archivist = setupProject(project, "archivist",
  localDependencies = Seq(common)
)

lazy val notifier = setupProject(project, "notifier",
  localDependencies = Seq(common, display),
  externalDependencies = ExternalDependencies.wiremockDependencies
)

lazy val bags_common = setupProject(project, "bags_common",
  localDependencies = Seq(common)
)

lazy val bags = setupProject(project, "bags",
  localDependencies = Seq(bags_common),
  externalDependencies = ExternalDependencies.circeOpticsDependencies
)

lazy val bags_api = setupProject(project, "bags_api",
  localDependencies = Seq(bags_common, display),
  externalDependencies = ExternalDependencies.circeOpticsDependencies
)

lazy val ingests_common = setupProject(project, "ingests_common",
  localDependencies = Seq(common)
)

lazy val ingests = setupProject(project, "ingests",
  localDependencies = Seq(ingests_common),
  externalDependencies = ExternalDependencies.wiremockDependencies
)

lazy val ingests_api = setupProject(project, "ingests_api",
  localDependencies = Seq(ingests_common, display),
  externalDependencies = ExternalDependencies.circeOpticsDependencies
)

lazy val bag_replicator = setupProject(project, "bag_replicator",
  localDependencies = Seq(common)
)

lazy val bag_verifier = setupProject(project, "bag_verifier",
  localDependencies = Seq(common)
)