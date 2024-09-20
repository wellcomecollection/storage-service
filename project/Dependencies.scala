import sbt._

object WellcomeDependencies {
  val defaultVersion = "32.42.1" // This is automatically bumped by the scala-libs release process, do not edit this line manually

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
    "org.wellcomecollection" %% name % version,
    "org.wellcomecollection" %% name % version % "test" classifier "tests"
  )
}

object ExternalDependencies {
  lazy val versions = new {

    val azure = "12.25.4"
    val commonsCompress = "1.27.1"
    val commonsIO = "2.17.0"
    val mockito = "5.13.0"
    val scalatest = "3.2.19"
    val scalatestPlus = "3.1.2.0"
    val scalatestPlusMockitoArtifactId = "mockito-3-2"

    // This should match the version of circe used in scala-json; see
    // https://github.com/wellcomecollection/scala-json/blob/master/project/Dependencies.scala
    val circeOptics = "0.14.1"

    // This should match the version of aws used in scala-libs; see
    // https://github.com/wellcomecollection/scala-libs/blob/main/project/Dependencies.scala
    val aws = "2.25.28"

    // These are the "Common Runtime Libraries", which you're encouraged to use for
    // better performance.
    // See https://docs.aws.amazon.com/sdkref/latest/guide/common-runtime.html
    val awsCrt = "0.29.25"
  }

  val azureDependencies: Seq[ModuleID] = Seq(
    "com.azure" % "azure-storage-blob" % versions.azure
  )

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

  val scalatestPlusDependencies = Seq(
    "org.scalatestplus" %% versions.scalatestPlusMockitoArtifactId % versions.scalatestPlus % Test
  )

  val mockitoDependencies: Seq[ModuleID] = Seq(
    "org.mockito" % "mockito-core" % versions.mockito % "test"
  )

  val nettyDependencies: Seq[ModuleID] = Seq(
    "io.netty" % "netty-tcnative" % "2.0.66.Final"
  )

  val awsTransferManagerDependencies: Seq[ModuleID] = Seq(
    "software.amazon.awssdk" % "s3-transfer-manager" % versions.aws,
    "software.amazon.awssdk.crt" % "aws-crt" % versions.awsCrt
  )
}

object StorageDependencies {
  val commonDependencies =
    ExternalDependencies.azureDependencies ++
      ExternalDependencies.commonsIODependencies ++
      ExternalDependencies.scalatestDependencies ++
      ExternalDependencies.scalatestPlusDependencies ++
      ExternalDependencies.mockitoDependencies ++
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

  val bagReplicatorDependencies =
    ExternalDependencies.awsTransferManagerDependencies ++
      // Note: the netty dependencies here are an attempt to fix an issue we saw where the
      // bag replicator was unable to start with the following error:
      //
      //      java.lang.ClassNotFoundException: io.netty.internal.tcnative.AsyncSSLPrivateKeyMethod
      //
      // See https://github.com/wellcomecollection/storage-service/issues/1066
      ExternalDependencies.nettyDependencies
}
