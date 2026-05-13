
sys.env.get("CODEARTIFACT_AUTH_TOKEN").filter(_.nonEmpty).foreach { token =>
  resolvers += "CodeArtifact" at "https://wellcomecollection-maven-mirror-760097843905.d.codeartifact.eu-west-1.amazonaws.com/maven/wellcomecollection-maven-mirror/"
  credentials += Credentials(
    "wellcomecollection-maven-mirror/wellcomecollection-maven-mirror",
    "wellcomecollection-maven-mirror-760097843905.d.codeartifact.eu-west-1.amazonaws.com",
    "aws",
    token
  )
}

addSbtPlugin("com.tapad" % "sbt-docker-compose" % "1.0.35")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")
addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.16")
addDependencyTreePlugin
