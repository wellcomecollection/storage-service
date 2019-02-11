import java.io.File

def setupProject(
                  project: Project,
                  folder: String,
                  localDependencies: Seq[Project] = Seq(),
                  externalDependencies: Seq[ModuleID] = Seq()
                ): Project = {

  // And here we actually create the project, with a few convenience wrappers
  // to make defining projects below cleaner.
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
  externalDependencies = StorageDependencies.sharedDependencies)

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