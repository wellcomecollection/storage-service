package uk.ac.wellcome.platform.storage.bags.api

import java.net.URL

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import uk.ac.wellcome.monitoring.typesafe.CloudWatchBuilder
import uk.ac.wellcome.platform.archive.bag_tracker.client.{
  AkkaBagTrackerClient,
  BagTrackerClient
}
import uk.ac.wellcome.platform.archive.common.config.builders._
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.http.{
  HttpMetrics,
  WellcomeHttpApp
}
import uk.ac.wellcome.platform.archive.common.storage.services.S3Uploader
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.typesafe.S3Builder
import uk.ac.wellcome.typesafe.WellcomeTypesafeApp
import uk.ac.wellcome.typesafe.config.builders.AkkaBuilder
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Main extends WellcomeTypesafeApp {
  val defaultCacheDuration = 1 days
  // The size here is dictated by the AWS API Gateway limits:
  // https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html
  // 9MB = 1048576 Bytes * 9
  val defaultMaxByteLength = 1048576 * 9

  runWithConfig { config: Config =>
    implicit val asMain: ActorSystem =
      AkkaBuilder.buildActorSystem()

    implicit val ecMain: ExecutionContext =
      AkkaBuilder.buildExecutionContext()

    implicit val matMain: Materializer =
      AkkaBuilder.buildMaterializer()

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

    val router: BagsApi = new BagsApi {
      override val httpServerConfig: HTTPServerConfig =
        HTTPServerBuilder.buildHTTPServerConfig(config)
      override implicit val ec: ExecutionContext = ecMain
      override val contextURL: URL = contextURLMain

      override val bagTrackerClient: BagTrackerClient =
        new AkkaBagTrackerClient(
          trackerHost = config.requireString("bags.tracker.host")
        )

      override val s3Uploader: S3Uploader = uploader
      override val maximumResponseByteLength: Long = defaultMaxByteLength
      override val s3Prefix: ObjectLocationPrefix = locationPrefix
      override val cacheDuration: Duration = defaultCacheDuration
      override implicit val materializer: Materializer = matMain
    }

    val appName = "BagsApi"

    new WellcomeHttpApp(
      routes = router.bags,
      httpMetrics = new HttpMetrics(
        name = appName,
        metrics = CloudWatchBuilder.buildCloudWatchMetrics(config)
      ),
      httpServerConfig = HTTPServerBuilder.buildHTTPServerConfig(config),
      contextURL = contextURLMain,
      appName = appName
    )
  }
}
