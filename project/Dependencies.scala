import sbt._

object WellcomeDependencies {
  val defaultVersion = "32.15.0" // This is automatically bumped by the scala-libs release process, do not edit this line manually

  lazy val versions = new {
    val fixtures = defaultVersion
    val http = defaultVersion
    val json = defaultVersion
    val elasticsearch = defaultVersion
    val messaging = defaultVersion
    val monitoring = defaultVersion
    val storage = defaultVersion
    val typesafe = defaultVersion
  }

  val jsonLibrary: Seq[ModuleID] = library(
    name = "json",
    version = versions.json
  )

  val fixturesLibrary: Seq[ModuleID] = library(
    name = "fixtures",
    version = versions.fixtures
  )

  val elasticsearchLibrary: Seq[ModuleID] = library(
    name = "elasticsearch",
    version = versions.elasticsearch
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

  val httpLibrary: Seq[ModuleID] = library(
    name = "http",
    version = versions.http
  )

  val httpTypesafeLibrary: Seq[ModuleID] = library(
    name = "http_typesafe",
    version = versions.http
  )

  val typesafeLibrary: Seq[ModuleID] = library(
    name = "typesafe_app",
    version = versions.typesafe
  ) ++ fixturesLibrary

  val monitoringTypesafeLibrary: Seq[ModuleID] = library(
    name = "monitoring_typesafe",
    version = versions.monitoring
  )

  val elasticsearchTypesafeLibrary: Seq[ModuleID] = library(
    name = "elasticsearch_typesafe",
    version = versions.elasticsearch
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
    "weco" %% name % version,
    "weco" %% name % version % "test" classifier "tests"
  )
}

object ExternalDependencies {
  lazy val versions = new {

    val commonsCompress = "1.5"
    val commonsIO = "2.6"
    val mockito = "1.9.5"
    val scalatest = "3.2.3"

    // This should match the version of circe used in scala-json; see
    // https://github.com/wellcomecollection/scala-json/blob/master/project/Dependencies.scala
    val circeOptics = "0.13.0"
  }

  val commonsCompressDependencies = Seq(
    "org.apache.commons" % "commons-compress" % versions.commonsCompress
  )

  val commonsIODependencies = Seq(
    "commons-io" % "commons-io" % versions.commonsIO
  )

  val circeOpticsDependencies: Seq[sbt.ModuleID] = Seq[ModuleID](
    "io.circe" %% "circe-optics" % versions.circeOptics
  )

  val scalatestDependencies = Seq[ModuleID](
    "org.scalatest" %% "scalatest" % versions.scalatest % "test"
  )

  val mockitoDependencies: Seq[ModuleID] = Seq(
    "org.mockito" % "mockito-core" % versions.mockito % "test"
  )
}

object StorageDependencies {
  val commonDependencies =
    ExternalDependencies.commonsIODependencies ++
      ExternalDependencies.scalatestDependencies ++
      WellcomeDependencies.jsonLibrary ++
      WellcomeDependencies.httpLibrary ++
      WellcomeDependencies.messagingLibrary ++
      WellcomeDependencies.monitoringLibrary ++
      WellcomeDependencies.storageLibrary ++
      WellcomeDependencies.typesafeLibrary ++
      WellcomeDependencies.monitoringTypesafeLibrary ++
      WellcomeDependencies.messagingTypesafeLibrary ++
      WellcomeDependencies.storageTypesafeLibrary ++
      WellcomeDependencies.httpTypesafeLibrary
}
