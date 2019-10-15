package uk.ac.wellcome.platform.storage.bags.api

import java.net.URL

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import uk.ac.wellcome.monitoring.typesafe.MetricsBuilder
import uk.ac.wellcome.platform.archive.common.config.builders._
import uk.ac.wellcome.platform.archive.common.http.{HttpMetrics, WellcomeHttpApp}
import uk.ac.wellcome.platform.archive.common.storage.services.{S3Uploader, StorageManifestDao}
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Main extends WellcomeTypesafeApp {
  runWithConfig { config: Config =>
    implicit val asMain: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val ecMain: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val amMain: ActorMaterializer =
      AkkaBuilder.buildActorMaterializer()

    val contextURLMain: URL =
      HTTPServerBuilder.buildContextURL(config)

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    val uploader = new S3Uploader()

    val s3Config = S3Builder.buildS3Config(
      config,
      namespace = "responses"
    )

    val locationPrefix = ObjectLocationPrefix(
      s3Config.bucketName,
      "responses"
    )

    val defaultCacheDuration = 30 days
    // 9MB = 1048576 Bytes * 9
    val defaultMaxByteLength = 1048576 * 9

    val router: BagsApi = new BagsApi {
      override implicit val ec: ExecutionContext = ecMain
      override val contextURL: URL = contextURLMain
      override val storageManifestDao: StorageManifestDao =
        StorageManifestDaoBuilder.build(config)

      override val s3Uploader: S3Uploader = uploader
      override val maximumResponseByteLength: Long = defaultMaxByteLength
      override val prefix: ObjectLocationPrefix = locationPrefix
      override val cacheDuration: Duration = defaultCacheDuration
      override implicit val materializer: ActorMaterializer = amMain
    }

    val appName = "BagsApi"

    new WellcomeHttpApp(
      routes = router.bags,
      httpMetrics = new HttpMetrics(
        name = appName,
        metrics = MetricsBuilder.buildMetricsSender(config)
      ),
      httpServerConfig = HTTPServerBuilder.buildHTTPServerConfig(config),
      contextURL = contextURLMain,
      appName = appName
    )
  }
}
