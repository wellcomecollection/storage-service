package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import java.net.URL

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.storage.ingests.api.IngestsApi
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestsApiFixture
  extends IngestStarterFixture
  with IngestGenerators
  with HttpFixtures
  with Messaging {

  val contextURL = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json")

  val metricsName = "IngestsApiFixture"

  private def withApp[R](
    table: Table,
    unpackerMessageSender: MemoryMessageSender,
    metricsSender: MetricsSender)(testWith: TestWith[IngestsApi[String], R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metricsSender = metricsSender
        )

        withIngestTracker(table) { ingestTracker =>
          withIngestStarter(table, unpackerMessageSender) { ingestStarter =>
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
    }

  def withBrokenApp[R](
    testWith: TestWith[(Table, MemoryMessageSender, MetricsSender, String), R]): R = {
      val messageSender = createMessageSender
      val table = Table("does-not-exist", index = "does-not-exist")
      withMockMetricsSender { metricsSender =>
        withApp(table, messageSender, metricsSender) { _ =>
          testWith(
            (
              table,
              messageSender,
              metricsSender,
              httpServerConfig.externalBaseURL))
        }
      }
    }

  def withConfiguredApp[R](
    testWith: TestWith[(Table, MemoryMessageSender, MetricsSender, String), R]): R =
    withIngestTrackerTable { table =>
      val messageSender = createMessageSender
      withMockMetricsSender { metricsSender =>
        withApp(table, messageSender, metricsSender) { _ =>
          testWith(
            (
              table,
              messageSender,
              metricsSender,
              httpServerConfig.externalBaseURL))
        }
      }
    }
}
