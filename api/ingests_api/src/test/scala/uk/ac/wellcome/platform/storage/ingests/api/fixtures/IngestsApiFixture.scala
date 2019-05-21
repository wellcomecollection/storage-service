package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import java.net.URL

import org.scalatest.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.MemoryIngestTracker
import uk.ac.wellcome.platform.storage.ingests.api.IngestsApi

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Try}

trait IngestsApiFixture
    extends IngestGenerators
    with HttpFixtures
    with IngestsStarterFixtures
    with MetricsSenderFixture { this: Matchers =>

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

        val ingestsApi = new IngestsApi(
          ingestTracker = ingestTracker,
          unpackerMessageSender = unpackerMessageSender,
          httpMetrics = httpMetrics,
          httpServerConfig = httpServerConfig,
          contextURL = contextURL
        )

        ingestsApi.run()

        testWith(ingestsApi)
      }
    }

  def withBrokenApp[R](
    testWith: TestWith[(MemoryIngestTracker,
                        MemoryMessageSender,
                        MetricsSender,
                        String),
                       R]): R = {
    val brokenTracker = new MemoryIngestTracker() {
      override def get(id: IngestID): Try[Option[Ingest]] =
        Failure(new Throwable("BOOM! get()"))

      override def initialise(ingest: Ingest): Try[Ingest] =
        Failure(new Throwable("BOOM! initialise()"))
    }

    val messageSender = createMessageSender

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

  def withConfiguredApp[R](
    testWith: TestWith[(MemoryIngestTracker,
                        MemoryMessageSender,
                        MetricsSender,
                        String),
                       R]): R = {
    val tracker = createIngestTracker
    val messageSender = createMessageSender

    withMockMetricsSender { metricsSender =>
      withApp(tracker, messageSender, metricsSender) { _ =>
        testWith(
          (
            tracker,
            messageSender,
            metricsSender,
            httpServerConfig.externalBaseURL))
      }
    }
  }
}
