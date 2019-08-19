package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import java.net.URL

import com.amazonaws.services.cloudwatch.model.StandardUnit
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.Metrics
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTrackerStoreError
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.storage.ingests.api.IngestsApi
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}
import uk.ac.wellcome.storage.{StoreWriteError, Version}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait IngestsApiFixture
    extends IngestStarterFixture
    with IngestGenerators
    with HttpFixtures
    with MetricsSenderFixture
    with IngestTrackerFixtures {

  val contextURLTest = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json"
  )

  val metricsName = "IngestsApiFixture"

  private def withApp[R](
    ingestTracker: MemoryIngestTracker,
    unpackerMessageSender: MemoryMessageSender,
    metrics: Metrics[Future, StandardUnit]
  )(testWith: TestWith[IngestsApi[String], R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metrics = metrics
        )

        withIngestStarter(ingestTracker, unpackerMessageSender) {
          ingestStarter =>
            val ingestsApi = new IngestsApi(
              ingestTracker = ingestTracker,
              ingestStarter = ingestStarter,
              httpMetrics = httpMetrics,
              httpServerConfig = httpServerConfig,
              contextURL = contextURLTest
            )

            ingestsApi.run()

            testWith(ingestsApi)
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
        Left(IngestTrackerStoreError(StoreWriteError(new Throwable("BOOM!"))))

      override def init(ingest: Ingest): Result =
        Left(IngestTrackerStoreError(StoreWriteError(new Throwable("BOOM!"))))
    }

    val metrics = new MemoryMetrics[StandardUnit]()

    withApp(brokenTracker, messageSender, metrics) { _ =>
      testWith(
        (
          brokenTracker,
          messageSender,
          metrics,
          httpServerConfig.externalBaseURL
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
            httpServerConfig.externalBaseURL
          )
        )
      }
    }
}
