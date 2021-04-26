import sbt._

object WellcomeDependencies {
  lazy val defaultVersion = "26.6.0"

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
    version = versions.json
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
    "uk.ac.wellcome" %% name % version,
    "uk.ac.wellcome" %% name % version % "test" classifier "tests"
  )
}

object ExternalDependencies {
  lazy val versions = new {

    val commonsCompress = "1.5"
    val commonsIO = "2.6"
    val mockito = "1.9.5"
    val scalatest = "3.2.3"
    val wiremock = "2.18.0"

    // This should match the version of circe used in scala-json; see
    // https://github.com/wellcomecollection/scala-json/blob/master/project/Dependencies.scala
    val circeOptics = "0.13.0"

    // Getting the akka-http dependencies right can be fiddly and takes some work.
    // In particular you need to use the same version of akka-http everywhere, or you
    // get errors (from LArgeResponsesTest) like:
    //
    //      Detected possible incompatible versions on the classpath. Please note that
    //      a given Akka HTTP version MUST be the same across all modules of Akka HTTP
    //      that you are using, e.g. if you use [10.1.10] all other modules that are
    //      released together MUST be of the same version.
    //
    //      Make sure you're using a compatible set of libraries.
    //
    // To work this out:
    //
    //   1. Look at the version of alpakka-streams used by scala-libs:
    //      https://github.com/wellcomecollection/scala-libs/blob/master/project/Dependencies.scala
    //      (At time of writing, v1.1.2)
    //
    //   2. Look at the corresponding akka-http dependency in alpakka:
    //      https://github.com/akka/alpakka/blob/master/project/Dependencies.scala
    //      (At time of writing, alpakka v1.1.2 pulls in akka-http 10.1.10)
    //
    //   3. Look at versions of akka-http-json.  Browse the Git tags until you find
    //      one that uses the same version of akka-http and a compatible Circe:
    //      https://github.com/hseeberger/akka-http-json/blob/master/build.sbt
    //
    val akkaHttpCirce = "1.29.1"
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

  val akkaDependencies: Seq[sbt.ModuleID] = Seq[ModuleID](
    "de.heikoseeberger" %% "akka-http-circe" % versions.akkaHttpCirce
  )

  val mockitoDependencies: Seq[ModuleID] = Seq(
    "org.mockito" % "mockito-core" % versions.mockito % "test"
  )

  val wiremockDependencies = Seq[ModuleID](
    "com.github.tomakehurst" % "wiremock" % versions.wiremock % "test"
  )
}

object StorageDependencies {
  val commonDependencies =
    ExternalDependencies.commonsIODependencies ++
      ExternalDependencies.akkaDependencies ++
      ExternalDependencies.scalatestDependencies ++
      WellcomeDependencies.jsonLibrary ++
      WellcomeDependencies.messagingLibrary ++
      WellcomeDependencies.monitoringLibrary ++
      WellcomeDependencies.storageLibrary ++
      WellcomeDependencies.typesafeLibrary ++
      WellcomeDependencies.monitoringTypesafeLibrary ++
      WellcomeDependencies.messagingTypesafeLibrary ++
      WellcomeDependencies.storageTypesafeLibrary ++
      WellcomeDependencies.httpLibrary
}
