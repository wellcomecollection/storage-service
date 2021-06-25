package weco.storage_service.ingests_worker

import java.net.URL

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.QueuePair
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models.IngestUpdate
import weco.storage_service.ingests_worker.fixtures.IngestsWorkerFixtures
import weco.http.fixtures.HttpFixtures

class IngestsWorkerFeatureTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with HttpFixtures
    with IngestsWorkerFixtures
    with IngestGenerators
    with IntegrationPatience {

  override def contextUrl = new URL("http://www.example.com")

  val visibilityTimeoutInSeconds = 1

  it("When the client succeeds it consumes the message") {
    withLocalSqsQueuePair(visibilityTimeoutInSeconds) {
      case QueuePair(queue, dlq) =>
        val client = successfulClient(ingest)

        withIngestWorker(queue, client) { _ =>
          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          eventually {
            assertQueueEmpty(queue)
            assertQueueEmpty(dlq)
          }
        }
    }
  }

  it("When the client conflicts it consumes the message") {
    withLocalSqsQueuePair(visibilityTimeoutInSeconds) {
      case QueuePair(queue, dlq) =>
        val client = conflictClient(ingestStatusUpdate)

        withIngestWorker(queue, client) { _ =>
          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          eventually {
            assertQueueEmpty(queue)
            assertQueueEmpty(dlq)
          }
        }
    }
  }

  it("When the client errors it does NOT consume the message") {
    withLocalSqsQueuePair(visibilityTimeoutInSeconds) {
      case QueuePair(queue, dlq) =>
        val client = unknownErrorClient(ingestStatusUpdate)

        withIngestWorker(queue, client) { _ =>
          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          eventually {
            getMessages(queue) shouldBe empty
            assertQueueHasSize(dlq, size = 1)
          }
        }
    }
  }

  it("When the client fails it does NOT consume the message") {
    withLocalSqsQueuePair(visibilityTimeoutInSeconds) {
      case QueuePair(queue, dlq) =>
        val client = failedFutureClient()

        withIngestWorker(queue, client) { _ =>
          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          eventually {
            getMessages(queue) shouldBe empty
            getMessages(dlq).length shouldBe 1
          }
        }
    }
  }
}
