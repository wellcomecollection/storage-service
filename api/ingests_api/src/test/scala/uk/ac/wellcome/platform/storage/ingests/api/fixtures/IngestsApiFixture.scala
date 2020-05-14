package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import java.net.URL

import com.amazonaws.services.cloudwatch.model.StandardUnit
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.Metrics
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.http.{
  HttpMetrics,
  WellcomeHttpApp
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.storage.ingests.api.IngestsApi
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture

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
    ingestTrackerTest: MemoryIngestTracker,
    unpackerSender: MemoryMessageSender,
    metrics: Metrics[Future, StandardUnit]
  )(testWith: TestWith[WellcomeHttpApp, R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metrics = metrics
        )

        val ingestsApi = new IngestsApi[String] {
          override implicit val ec: ExecutionContext = global
          override val ingestTrackerClient: IngestTrackerClient =
            new AkkaIngestTrackerClient(trackerUri)

          override val unpackerMessageSender: MessageSender[String] =
            unpackerSender

          override val httpServerConfig: HTTPServerConfig =
            httpServerConfigTest
          override val contextURL: URL = contextURLTest
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
    }

  def withBrokenApp[R](
    testWith: TestWith[
      (
        MemoryIngestTracker,
        MemoryMessageSender,
        MemoryMetrics[StandardUnit],
        String
      ),
      R
    ]
  ): R =
    withBrokenIngestsTrackerApi {
      case (_, _, ingestTracker) =>
        val messageSender = new MemoryMessageSender()

        val metrics = new MemoryMetrics[StandardUnit]()

        withApp(ingestTracker, messageSender, metrics) { _ =>
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
        MemoryMetrics[StandardUnit],
        String
      ),
      R
    ]
  ): R =
    withIngestsTrackerApi(initialIngests = initialIngests) {
      case (_, _, ingestTracker) =>
        val messageSender = new MemoryMessageSender()

        val metrics = new MemoryMetrics[StandardUnit]()

        withApp(ingestTracker, messageSender, metrics) { _ =>
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
