import sbt._

object WellcomeDependencies {
  lazy val versions = new {
    val fixtures   = "1.0.0"
    val json       = "1.1.1"
    val messaging  = "5.3.1"
    val monitoring = "2.3.0"
    val storage = "7.18.6"
    val typesafe = "1.0.0"
  }

  val jsonLibrary: Seq[ModuleID] = library(
    name = "json",
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

  val monitoringTypesafeLibrary: Seq[ModuleID] = library(
    name = "monitoring_typesafe",
    version = versions.monitoring
  )

  val messagingTypesafeLibrary: Seq[ModuleID] = library(
    name = "messaging_typesafe",
    version = versions.messaging
  )

  val storageTypesafeLibrary: Seq[ModuleID] = library(
    name = "storage_typesafe",
    version = versions.storage
  )

  private def library(name: String, version: String): Seq[ModuleID] = Seq(
    "uk.ac.wellcome" %% name % version,
    "uk.ac.wellcome" %% name % version % "test" classifier "tests"
  )
}

object ExternalDependencies {
  lazy val versions = new {
    val akkaHttp = "10.1.5"
    val akkaHttpCirce = "1.21.1"
    val akkaStreamAlpakka = "0.20"

    val commonsCompress = "1.5"
    val commonsIO = "2.6"

    val aws = "1.11.95"

    val circe = "0.9.0"

    val scalatest = "3.0.1"
    val wiremock = "2.18.0"

    val logback = "1.2.3"
    val logstashLogback ="6.1"
  }

  val loggingDependencies = Seq(
    "ch.qos.logback" % "logback-classic" % versions.logback,
    "ch.qos.logback" % "logback-core" % versions.logback,
    "ch.qos.logback" % "logback-access" % versions.logback,

    "net.logstash.logback" % "logstash-logback-encoder" % versions.logstashLogback
  )

  val commonsCompressDependencies = Seq(
    "org.apache.commons" % "commons-compress" % versions.commonsCompress
  )

  val commonsIODependencies = Seq(
    "commons-io" % "commons-io" % versions.commonsIO
  )

  val circeOpticsDependencies = Seq[ModuleID](
    "io.circe" %% "circe-optics" % versions.circe % "test"
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
    ExternalDependencies.commonsIODependencies ++
      ExternalDependencies.akkaDependencies ++
      ExternalDependencies.cloudwatchMetricsDependencies ++
      ExternalDependencies.scalatestDependencies ++
      ExternalDependencies.loggingDependencies ++
      WellcomeDependencies.jsonLibrary ++
      WellcomeDependencies.messagingLibrary ++
      WellcomeDependencies.monitoringLibrary ++
      WellcomeDependencies.storageLibrary ++
      WellcomeDependencies.typesafeLibrary ++
      WellcomeDependencies.monitoringTypesafeLibrary ++
      WellcomeDependencies.messagingTypesafeLibrary ++
      WellcomeDependencies.storageTypesafeLibrary
}
