import sbt._

object WellcomeDependencies {
  lazy val versions = new {
    val fixtures = "1.2.0"
    val json = "2.3.0"
    val messaging = "9.1.0"
    val monitoring = "4.0.0"
    val storage = "8.1.0"
    val typesafe = "2.0.0"
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



    val apacheLogging = "2.8.2"
    val commonsCompress = "1.5"
    val commonsIO = "2.6"
    val elastic4s = "7.6.1"
    val mockito = "1.9.5"
    val aws = "1.11.504"
    val scalatest = "3.1.1"
    val wiremock = "2.18.0"
    val logback = "1.2.3"
    val logstashLogback = "6.1"

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
    //   1. Look at the version of alpakka-streams used by scala-messaging:
    //      https://github.com/wellcomecollection/scala-messaging/blob/master/project/Dependencies.scala
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
    val akkaStreamAlpakka = "1.1.2"
    val akkaHttp = "10.1.10"
    val akkaHttpCirce = "1.29.1"
  }

  val logbackDependencies = Seq(
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

  val circeOpticsDependencies: Seq[sbt.ModuleID] = Seq[ModuleID](
    "io.circe" %% "circe-optics" % versions.circeOptics
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

  val elasticsearchDependencies: Seq[ModuleID] = Seq(
    "org.apache.logging.log4j" % "log4j-core" % versions.apacheLogging,
    "org.apache.logging.log4j" % "log4j-api" % versions.apacheLogging,
    "com.sksamuel.elastic4s" %% "elastic4s-core" % versions.elastic4s,
    "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % versions.elastic4s,
    "com.sksamuel.elastic4s" %% "elastic4s-http-streams" % versions.elastic4s,
    "com.sksamuel.elastic4s" %% "elastic4s-testkit" % versions.elastic4s % "test"
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
      ExternalDependencies.cloudwatchMetricsDependencies ++
      ExternalDependencies.scalatestDependencies ++
      ExternalDependencies.logbackDependencies ++
      WellcomeDependencies.jsonLibrary ++
      WellcomeDependencies.messagingLibrary ++
      WellcomeDependencies.monitoringLibrary ++
      WellcomeDependencies.storageLibrary ++
      WellcomeDependencies.typesafeLibrary ++
      WellcomeDependencies.monitoringTypesafeLibrary ++
      WellcomeDependencies.messagingTypesafeLibrary ++
      WellcomeDependencies.storageTypesafeLibrary
}
