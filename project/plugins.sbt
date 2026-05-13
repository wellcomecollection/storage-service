
{
  val mode = if (sys.env.get("CODEARTIFACT_AUTH_TOKEN").exists(_.nonEmpty))
    "CodeArtifact → Maven Central"
  else
    "Maven Central only"
  println(s"[info] Plugin resolution: $mode")
}

resolvers ++= sys.env.get("CODEARTIFACT_AUTH_TOKEN").filter(_.nonEmpty).map(_ =>
  "CodeArtifact" at "https://wellcomecollection-maven-mirror-760097843905.d.codeartifact.eu-west-1.amazonaws.com/maven/wellcomecollection-maven-mirror/"
).toSeq

credentials ++= sys.env.get("CODEARTIFACT_AUTH_TOKEN").filter(_.nonEmpty).map(token =>
  Credentials(
    "wellcomecollection-maven-mirror/wellcomecollection-maven-mirror",
    "wellcomecollection-maven-mirror-760097843905.d.codeartifact.eu-west-1.amazonaws.com",
    "aws",
    token
  )
).toSeq

addSbtPlugin("com.tapad" % "sbt-docker-compose" % "1.0.35")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")
addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.16")
addDependencyTreePlugin
