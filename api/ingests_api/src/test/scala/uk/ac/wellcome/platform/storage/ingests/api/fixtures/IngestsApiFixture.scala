package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import java.net.URL

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTrackerStoreError
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.storage.ingests.api.IngestsApi
import uk.ac.wellcome.storage.{StoreWriteError, Version}
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestsApiFixture
    extends IngestStarterFixture
    with IngestGenerators
    with HttpFixtures
    with MetricsSenderFixture
    with IngestTrackerFixtures {

  val contextURL = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json")

  val metricsName = "IngestsApiFixture"

  private def withApp[R](ingestTracker: MemoryIngestTracker,
                         unpackerMessageSender: MemoryMessageSender,
                         metricsSender: MetricsSender)(
    testWith: TestWith[IngestsApi[String], R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metricsSender = metricsSender
        )

        withIngestStarter(ingestTracker, unpackerMessageSender) {
          ingestStarter =>
            val ingestsApi = new IngestsApi(
              ingestTracker = ingestTracker,
              ingestStarter = ingestStarter,
              httpMetrics = httpMetrics,
              httpServerConfig = httpServerConfig,
              contextURL = contextURL
            )

            ingestsApi.run()

            testWith(ingestsApi)
        }
      }
    }

  def withBrokenApp[R](
    testWith: TestWith[(MemoryIngestTracker,
                        MemoryMessageSender,
                        MetricsSender,
                        String),
                       R]): R = {
    val messageSender = new MemoryMessageSender()

    val brokenTracker = new MemoryIngestTracker(
      underlying = new MemoryVersionedStore[IngestID, Int, Ingest](
        new MemoryStore[Version[IngestID, Int], Ingest](
          initialEntries = Map.empty) with MemoryMaxima[IngestID, Ingest]
      )
    ) {
      override def get(id: IngestID): Result =
        Left(IngestTrackerStoreError(StoreWriteError(new Throwable("BOOM!"))))
    }

    withMockMetricsSender { metricsSender =>
      withApp(brokenTracker, messageSender, metricsSender) { _ =>
        testWith(
          (
            brokenTracker,
            messageSender,
            metricsSender,
            httpServerConfig.externalBaseURL))
      }
    }
  }

  def withConfiguredApp[R](initialIngests: Seq[Ingest] = Seq.empty)(
    testWith: TestWith[(MemoryIngestTracker,
                        MemoryMessageSender,
                        MetricsSender,
                        String),
                       R]): R =
    withMemoryIngestTracker(initialIngests = initialIngests) { ingestTracker =>
      val messageSender = new MemoryMessageSender()
      withMockMetricsSender { metricsSender =>
        withApp(ingestTracker, messageSender, metricsSender) { _ =>
          testWith(
            (
              ingestTracker,
              messageSender,
              metricsSender,
              httpServerConfig.externalBaseURL))
        }
      }
    }
}
