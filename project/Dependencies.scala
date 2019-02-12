import sbt._

object WellcomeDependencies {
  lazy val versions = new {
    val fixtures   = "1.0.0"
    val json       = "1.1.1"
    val messaging  = "1.2.0"
    val monitoring = "1.3.0"
    val storage    = "3.3.0"
    val typesafe   = "1.0.0"
  }

  val jsonLibrary: Seq[ModuleID] = library(
    name    = "json",
    version = versions.json
  )

  val fixturesLibrary: Seq[ModuleID] = library(
    name = "fixtures",
    version = versions.fixtures
  )

  val messagingLibrary: Seq[ModuleID] = library(
    name = "messaging",
    version = versions.messaging
  )

  val monitoringLibrary: Seq[ModuleID] = library(
    name = "monitoring",
    version = versions.monitoring
  )

  val storageLibrary: Seq[ModuleID] = library(
    name = "storage",
    version = versions.storage
  )

  val typesafeLibrary: Seq[ModuleID] = library(
    name = "typesafe-app",
    version = versions.typesafe
  ) ++ fixturesLibrary

  val messagingTypesafeLibrary: Seq[ModuleID] = messagingLibrary ++ library(
    name = "messaging_typesafe",
    version = versions.messaging
  ) ++ monitoringLibrary ++ storageLibrary ++ typesafeLibrary

  private def library(name: String, version: String): Seq[ModuleID] = Seq(
    "uk.ac.wellcome" %% name % version,
    "uk.ac.wellcome" %% name % version % "test" classifier "tests"
  )
}

object ExternalDependencies {
  lazy val versions = new {
    val akkaHttp            = "10.1.5"
    val akkaHttpCirce       = "1.21.1"
    val akkaStreamAlpakka   = "0.20"
    val apacheCommons       = "2.6"
    val aws                 = "1.11.95"
    val circe               = "0.9.0"
    val mockito             = "1.9.5"
    val scalatest           = "3.0.1"
    val wiremock            = "2.18.0"
  }

  val apacheCommonsDependencies = Seq(
    "commons-io" % "commons-io" % versions.apacheCommons % "test"
  )

  val circeOpticsDependencies = Seq[ModuleID](
    "io.circe" %% "circe-optics" % versions.circe
  )

  val mockitoDependencies: Seq[ModuleID] = Seq(
    "org.mockito" % "mockito-core" % versions.mockito % "test"
  )

  val scalatestDependencies = Seq[ModuleID](
    "org.scalatest" %% "scalatest" % versions.scalatest % "test"
  )

  val akkaDependencies = Seq[ModuleID](
    "com.lightbend.akka" %% "akka-stream-alpakka-s3" % versions.akkaStreamAlpakka,
    "com.lightbend.akka" %% "akka-stream-alpakka-sns" % versions.akkaStreamAlpakka,
    "com.typesafe.akka" %% "akka-http" % versions.akkaHttp,
    "de.heikoseeberger" %% "akka-http-circe" % versions.akkaHttpCirce
  )

  val cloudwatchMetricsDependencies = Seq[ModuleID](
    "com.amazonaws" % "aws-java-sdk-cloudwatchmetrics" % versions.aws
  )

  val wiremockDependencies = Seq[ModuleID](
    "com.github.tomakehurst" % "wiremock" % versions.wiremock % "test"
  )
}

object StorageDependencies {
  val commonDependencies =
    ExternalDependencies.apacheCommonsDependencies ++
    ExternalDependencies.akkaDependencies ++
    ExternalDependencies.cloudwatchMetricsDependencies ++
    ExternalDependencies.mockitoDependencies ++
    ExternalDependencies.scalatestDependencies ++
    WellcomeDependencies.jsonLibrary ++
    WellcomeDependencies.messagingTypesafeLibrary
}
