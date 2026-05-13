import sbt.Keys._
import sbt._

object Common {
  private val codeArtifactToken = sys.env.get("CODEARTIFACT_AUTH_TOKEN").filter(_.nonEmpty)

  val settings: Seq[Def.Setting[_]] = Seq(
    scalaVersion := "2.12.16",
    organization := "weco",
    externalResolvers := {
      val codeArtifact = codeArtifactToken.map(_ =>
        "CodeArtifact" at "https://wellcomecollection-maven-mirror-760097843905.d.codeartifact.eu-west-1.amazonaws.com/maven/wellcomecollection-maven-mirror/"
      ).toSeq
      Seq(Resolver.defaultLocal) ++ codeArtifact ++ Seq(Resolver.DefaultMavenRepository)
    },
    credentials ++= codeArtifactToken.map(token =>
      Credentials(
        "wellcomecollection-maven-mirror/wellcomecollection-maven-mirror",
        "wellcomecollection-maven-mirror-760097843905.d.codeartifact.eu-west-1.amazonaws.com",
        "aws",
        token
      )
    ).toSeq,
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
    parallelExecution in Test := false,
    // Don't build scaladocs
    // https://www.scala-sbt.org/sbt-native-packager/formats/universal.html#skip-packagedoc-task-on-stage
    mappings in (Compile, packageDoc) := Nil
  )
}
