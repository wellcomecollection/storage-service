// Plugin resolution for the meta-build. Mirrors Common.scala, which only
// covers the application projects; plugins are resolved before it compiles.
val codeArtifactToken = sys.env.get("CODEARTIFACT_AUTH_TOKEN").filter(_.nonEmpty)

val codeArtifact: Seq[Resolver] = codeArtifactToken.map(_ =>
  "CodeArtifact" at "https://wellcomecollection-maven-mirror-760097843905.d.codeartifact.eu-west-1.amazonaws.com/maven/wellcomecollection-maven-mirror/"
).toSeq

// For plugin builds sbt composes fullResolvers as sbtResolvers ++ externalResolvers
// and dedupes, so overriding externalResolvers alone leaves the mirror last.
sbtResolvers := (Seq(Resolver.defaultLocal) ++ codeArtifact ++ sbtResolvers.value).distinct

credentials ++= codeArtifactToken.map(token =>
  Credentials(
    "wellcomecollection-maven-mirror/wellcomecollection-maven-mirror",
    "wellcomecollection-maven-mirror-760097843905.d.codeartifact.eu-west-1.amazonaws.com",
    "aws",
    token
  )
).toSeq
