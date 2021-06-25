package weco.storage_service.ingests_api.fixtures

import java.net.URL

import akka.http.scaladsl.model.HttpEntity
import io.circe.Decoder
import weco.fixtures.TestWith
import weco.json.JsonUtil.fromJson
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.Metrics
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models.Ingest
import weco.storage_service.ingests_api.IngestsApi
import weco.storage_service.ingests_api.services.IngestCreator
import weco.storage_service.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import weco.storage_service.ingests_tracker.fixtures.{
  IngestTrackerFixtures,
  IngestsTrackerApiFixture
}
import weco.storage_service.ingests_tracker.tracker.memory.MemoryIngestTracker
import weco.http.WellcomeHttpApp
import weco.http.fixtures.HttpFixtures
import weco.http.models.HTTPServerConfig
import weco.http.monitoring.HttpMetrics

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

trait IngestsApiFixture
    extends IngestGenerators
    with HttpFixtures
    with IngestTrackerFixtures
    with IngestsTrackerApiFixture {

  val contextURLTest = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json"
  )

  override def contextUrl = contextURLTest

  val metricsName = "IngestsApiFixture"

  private def withIngestsApi[R](
    unpackerSender: MemoryMessageSender,
    metrics: Metrics[Future]
  )(testWith: TestWith[WellcomeHttpApp, R]): R =
    withActorSystem { implicit actorSystem =>
      val httpMetrics = new HttpMetrics(
        name = metricsName,
        metrics = metrics
      )

      val client = new AkkaIngestTrackerClient(trackerUri)

      val ingestCreatorInstance = new IngestCreator(
        ingestTrackerClient = client,
        unpackerMessageSender = unpackerSender
      )

      val ingestsApi = new IngestsApi[String] {
        override implicit val ec: ExecutionContext = global
        override val ingestTrackerClient: IngestTrackerClient =
          client

        override val httpServerConfig: HTTPServerConfig =
          httpServerConfigTest
        override val contextUrl = contextURLTest
        override val ingestCreator: IngestCreator[String] =
          ingestCreatorInstance
      }

      withApp(ingestsApi.ingests, Some(httpMetrics), Some(actorSystem)) { app =>
        testWith(app)
      }
    }

  def withBrokenApp[R](
    testWith: TestWith[
      (
        MemoryIngestTracker,
        MemoryMessageSender,
        MemoryMetrics,
        String
      ),
      R
    ]
  ): R =
    withBrokenIngestsTrackerApi {
      case (_, _, ingestTracker) =>
        val messageSender = new MemoryMessageSender()

        val metrics = new MemoryMetrics()

        withIngestsApi(messageSender, metrics) { _ =>
          testWith(
            (
              ingestTracker,
              messageSender,
              metrics,
              httpServerConfigTest.externalBaseURL
            )
          )
        }
    }

  def withConfiguredApp[R](initialIngests: Seq[Ingest] = Seq.empty)(
    testWith: TestWith[
      (
        MemoryIngestTracker,
        MemoryMessageSender,
        MemoryMetrics,
        String
      ),
      R
    ]
  ): R =
    withIngestsTrackerApi(initialIngests = initialIngests) {
      case (_, _, ingestTracker) =>
        val messageSender = new MemoryMessageSender()

        val metrics = new MemoryMetrics()

        withIngestsApi(messageSender, metrics) { _ =>
          testWith(
            (
              ingestTracker,
              messageSender,
              metrics,
              httpServerConfigTest.externalBaseURL
            )
          )
        }
    }

  def getT[T](entity: HttpEntity)(implicit decoder: Decoder[T]): T =
    withMaterializer { implicit materializer =>
      val timeout = 300.millis

      val stringBody = entity
        .toStrict(timeout)
        .map(_.data)
        .map(_.utf8String)
        .value
        .get
        .get
      fromJson[T](stringBody).get
    }
}
