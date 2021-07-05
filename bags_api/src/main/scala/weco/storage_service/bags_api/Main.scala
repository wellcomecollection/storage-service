package weco.storage_service.bags_api

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import weco.http.typesafe.HTTPServerBuilder
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_tracker.client.{
  AkkaBagTrackerClient,
  BagTrackerClient
}
import weco.storage.s3.S3ObjectLocationPrefix
import weco.storage.services.s3.S3Uploader
import weco.storage.typesafe.S3Builder
import weco.typesafe.WellcomeTypesafeApp
import weco.typesafe.config.builders.AkkaBuilder
import weco.typesafe.config.builders.EnrichConfig._
import weco.http.WellcomeHttpApp
import weco.http.models.HTTPServerConfig
import weco.http.monitoring.HttpMetrics

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

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    val uploader = new S3Uploader()

    val s3Config = S3Builder.buildS3Config(
      config,
      namespace = "responses"
    )

    val locationPrefix = S3ObjectLocationPrefix(
      bucket = s3Config.bucketName,
      keyPrefix = "responses"
    )

    val client = new AkkaBagTrackerClient(
      trackerHost = config.requireString("bags.tracker.host")
    )

    val router: BagsApi = new BagsApi {
      override val httpServerConfig: HTTPServerConfig =
        HTTPServerBuilder.buildHTTPServerConfig(config)
      override implicit val ec: ExecutionContext = ecMain

      override val bagTrackerClient: BagTrackerClient = client

      override val s3Uploader: S3Uploader = uploader
      override val s3Prefix: S3ObjectLocationPrefix = locationPrefix

      override val maximumResponseByteLength: Long = defaultMaxByteLength
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
      appName = appName
    )
  }
}
