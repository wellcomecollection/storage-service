import java.io.File

import sbt.{IO, Project}
import io.circe.generic.auto._
import io.circe.syntax._

object Metadata {
  def write(
    project: Project,
    folder: String,
    localDependencies: Seq[Project] = Seq()
  ) = {
    // Here we write a bit of metadata about the project, and the other
    // local projects it depends on.  This can be used to determine whether
    // to run tests based on the up-to-date project graph.
    // See https://www.scala-sbt.org/release/docs/Howto-Generating-Files.html
    val file = new File(s".sbt_metadata/${project.id}.json")
    val dependencyIds: List[String] = localDependencies.map { p: Project =>
      p.id
    }.toList

    case class ProjectMetadata(
      id: String,
      folder: String,
      dependencyIds: List[String]
    )

    val metadata = ProjectMetadata(
      id = project.id,
      folder = folder,
      dependencyIds = dependencyIds
    )

    IO.write(file, metadata.asJson.spaces2)
  }
}
