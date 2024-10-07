import sbt._

object WellcomeDependencies {
  lazy val versions = new {
    val typesafe = "32.43.2"
    val fixtures = "32.43.2"
    val http = "32.43.2"
    val json = "32.43.2"
    val messaging = "32.43.2"
    val monitoring = "32.43.2"
    val storage = "32.43.2"
    val elasticsearch = "32.43.2"
    val sierra = "32.43.2"
  }

  val jsonLibrary: Seq[ModuleID] = Seq(
    "org.wellcomecollection" %% "json" % versions.json,
    "org.wellcomecollection" %% "json" % versions.json % "test" classifier "tests"
  )

  val fixturesLibrary: Seq[ModuleID] = Seq(
    "org.wellcomecollection" %% "fixtures" % versions.fixtures,
    "org.wellcomecollection" %% "fixtures" % versions.fixtures % "test" classifier "tests"
  )

  val messagingLibrary: Seq[ModuleID] = Seq(
    "org.wellcomecollection" %% "messaging" % versions.messaging,
    "org.wellcomecollection" %% "messaging" % versions.messaging % "test" classifier "tests"
  )

  val elasticsearchLibrary: Seq[ModuleID] =  Seq(
    "org.wellcomecollection" %% "elasticsearch" % versions.elasticsearch,
    "org.wellcomecollection" %% "elasticsearch" % versions.elasticsearch % "test" classifier "tests"
  )

  val elasticsearchTypesafeLibrary: Seq[ModuleID] = Seq(
    "org.wellcomecollection" %% "elasticsearch_typesafe" % versions.elasticsearch,
    "org.wellcomecollection" %% "elasticsearch_typesafe" % versions.elasticsearch % "test" classifier "tests"
  )

  val httpLibrary: Seq[ModuleID] = Seq(
    "org.wellcomecollection" %% "http" % versions.http,
    "org.wellcomecollection" %% "http" % versions.http % "test" classifier "tests"
  )

  val httpTypesafeLibrary: Seq[ModuleID] = Seq(
    "org.wellcomecollection" %% "http_typesafe" % versions.http,
    "org.wellcomecollection" %% "http_typesafe" % versions.http % "test" classifier "tests"
  )

  val monitoringLibrary: Seq[ModuleID] = Seq(
    "org.wellcomecollection" %% "monitoring" % versions.monitoring,
    "org.wellcomecollection" %% "monitoring" % versions.monitoring % "test" classifier "tests"
  )

  val monitoringTypesafeLibrary: Seq[ModuleID] = monitoringLibrary ++ Seq(
    "org.wellcomecollection" %% "monitoring_typesafe" % versions.monitoring,
    "org.wellcomecollection" %% "monitoring_typesafe" % versions.monitoring % "test" classifier "tests"
  )

  val storageLibrary: Seq[ModuleID] = Seq(
    "org.wellcomecollection" %% "storage" % versions.storage,
    "org.wellcomecollection" %% "storage" % versions.storage % "test" classifier "tests"
  )

  val typesafeLibrary: Seq[ModuleID] =  Seq(
    "org.wellcomecollection" %% "typesafe_app" % versions.typesafe,
    "org.wellcomecollection" %% "typesafe_app" % versions.typesafe % "test" classifier "tests"
  ) ++ fixturesLibrary

  val messagingTypesafeLibrary: Seq[ModuleID] = messagingLibrary ++ Seq(
    "org.wellcomecollection" %% "messaging_typesafe" % versions.messaging,
    "org.wellcomecollection" %% "messaging_typesafe" % versions.messaging % "test" classifier "tests"
  ) ++ monitoringLibrary

  val storageTypesafeLibrary: Seq[ModuleID] = storageLibrary ++ Seq(
    "org.wellcomecollection" %% "storage_typesafe" % versions.storage,
    "org.wellcomecollection" %% "storage_typesafe" % versions.storage % "test" classifier "tests"
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
    val aws = "2.25.70"

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
