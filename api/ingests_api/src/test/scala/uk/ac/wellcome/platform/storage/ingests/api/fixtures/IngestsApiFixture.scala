package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import java.net.URL

import com.amazonaws.services.cloudwatch.model.StandardUnit
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.Metrics
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.http.{
  HttpMetrics,
  WellcomeHttpApp
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestStoreUnexpectedError,
  IngestTracker
}
import uk.ac.wellcome.platform.storage.ingests.api.IngestsApi
import uk.ac.wellcome.platform.storage.ingests.api.services.IngestStarter
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}
import uk.ac.wellcome.storage.Version

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait IngestsApiFixture
    extends IngestStarterFixture
    with IngestGenerators
    with HttpFixtures
    with MetricsSenderFixture
    with IngestTrackerFixtures {

  override val contextURLTest = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json"
  )

  override val metricsName = "IngestsApiFixture"

  private def withApp[R](
    ingestTrackerTest: MemoryIngestTracker,
    unpackerMessageSender: MemoryMessageSender,
    metrics: Metrics[Future, StandardUnit]
  )(testWith: TestWith[WellcomeHttpApp, R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metrics = metrics
        )

        withIngestStarter(ingestTrackerTest, unpackerMessageSender) {
          ingestStarterTest =>
            val ingestsApi = new IngestsApi {
              override val ingestTracker: IngestTracker = ingestTrackerTest
              override val ingestStarter: IngestStarter[_] = ingestStarterTest
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
  ): R = {
    val messageSender = new MemoryMessageSender()

    val brokenTracker = new MemoryIngestTracker(
      underlying = new MemoryVersionedStore[IngestID, Ingest](
        new MemoryStore[Version[IngestID, Int], Ingest](
          initialEntries = Map.empty
        ) with MemoryMaxima[IngestID, Ingest]
      )
    ) {
      override def get(id: IngestID): Result =
        Left(IngestStoreUnexpectedError(new Throwable("BOOM!")))

      override def init(ingest: Ingest): Result =
        Left(IngestStoreUnexpectedError(new Throwable("BOOM!")))
    }

    val metrics = new MemoryMetrics[StandardUnit]()

    withApp(brokenTracker, messageSender, metrics) { _ =>
      testWith(
        (
          brokenTracker,
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
    withMemoryIngestTracker(initialIngests = initialIngests) { ingestTracker =>
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
