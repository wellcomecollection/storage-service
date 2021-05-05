package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import java.net.URL

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.Metrics
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.storage.ingests.api.IngestsApi
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestCreator
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.{
  IngestTrackerFixtures,
  IngestsTrackerApiFixture
}
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.memory.MemoryIngestTracker
import weco.http.WellcomeHttpApp
import weco.http.models.HTTPServerConfig
import weco.http.monitoring.HttpMetrics

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

trait IngestsApiFixture
    extends IngestGenerators
    with HttpFixtures
    with IngestTrackerFixtures
    with IngestsTrackerApiFixture {

  override val contextURLTest = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json"
  )

  override val metricsName = "IngestsApiFixture"

  private def withApp[R](
    unpackerSender: MemoryMessageSender,
    metrics: Metrics[Future]
  )(testWith: TestWith[WellcomeHttpApp, R]): R =
    withActorSystem { implicit actorSystem =>
      val httpMetrics = new HttpMetrics(
        name = metricsName,
        metrics = metrics
      )

      val ingestTrackerClient: IngestTrackerClient =
        new AkkaIngestTrackerClient(trackerUri)
      val ingestCreatorInstance = new IngestCreator(
        ingestTrackerClient = ingestTrackerClient,
        unpackerMessageSender = unpackerSender
      )
      val ingestsApi = new IngestsApi[String] {
        override implicit val ec: ExecutionContext = global
        override val ingestTrackerClient: IngestTrackerClient =
          new AkkaIngestTrackerClient(trackerUri)

        override val httpServerConfig: HTTPServerConfig =
          httpServerConfigTest
        override val context = contextURLTest.toString
        override val ingestCreator: IngestCreator[String] =
          ingestCreatorInstance
      }

      val app = new WellcomeHttpApp(
        routes = ingestsApi.ingests,
        httpMetrics = httpMetrics,
        httpServerConfig = httpServerConfigTest,
        contextURL = contextURLTest,
        appName = metricsName
      )

      app.run()

      testWith(app)
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

        withApp(messageSender, metrics) { _ =>
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

        withApp(messageSender, metrics) { _ =>
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
}
