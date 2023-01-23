package weco.storage_service.bags_api

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.typesafe.config.Config
import org.apache.commons.io.FileUtils
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import weco.http.typesafe.HTTPServerBuilder
import weco.monitoring.typesafe.CloudWatchBuilder
import weco.storage_service.bag_tracker.client.{
  AkkaBagTrackerClient,
  BagTrackerClient
}
import weco.storage.s3.S3ObjectLocationPrefix
import weco.storage.services.s3.{S3PresignedUrls, S3Uploader}
import weco.storage.typesafe.S3Builder
import weco.typesafe.WellcomeTypesafeApp
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
  val defaultMaxByteLength = 9 * FileUtils.ONE_MB

  runWithConfig { config: Config =>
    implicit val actorSystem: ActorSystem =
      ActorSystem("main-actor-system")

    implicit val ec: ExecutionContext =
      actorSystem.dispatcher

    implicit val s3Client: S3Client = S3Client.builder().build()
    implicit val s3Presigner: S3Presigner = S3Presigner.builder().build()

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
      override implicit val ec: ExecutionContext = actorSystem.dispatcher

      override val bagTrackerClient: BagTrackerClient = client

      override val s3PresignedUrls: S3PresignedUrls =
        new S3PresignedUrls()

      override val s3Uploader: S3Uploader = uploader
      override val s3Prefix: S3ObjectLocationPrefix = locationPrefix

      override val maximumResponseByteLength: Long = defaultMaxByteLength
      override val cacheDuration: Duration = defaultCacheDuration
      override implicit val materializer: Materializer =
        Materializer(actorSystem)
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
