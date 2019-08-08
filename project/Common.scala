import sbt.Keys._
import sbt._

object Common {
  val settings: Seq[Def.Setting[_]] = Seq(
    scalaVersion := "2.12.6",
    organization := "uk.ac.wellcome",
    resolvers ++= Seq(
      "S3 releases" at "s3://releases.mvn-repo.wellcomecollection.org/"
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-Xlint",
      "-Xverify",
      "-Xfatal-warnings",
      "-feature",
      "-language:postfixOps",
      "-Ypartial-unification",
      "-Xcheckinit"
    ),
    parallelExecution in Test := false
  )
}
