package uk.ac.wellcome.platform.storage.ingests_worker

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestUpdate
import uk.ac.wellcome.platform.storage.ingests_worker.fixtures.IngestsWorkerFixtures

class IngestsWorkerFeatureTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with HttpFixtures
    with IngestsWorkerFixtures
    with IngestGenerators
    with IntegrationPatience {

  val visibilityTimeoutInSeconds = 1
  val waitTimeInMilliseconds = (visibilityTimeoutInSeconds + 1) * 1000

  it("When the client succeeds it should consume the message") {
    withLocalSqsQueueAndDlqAndTimeout(visibilityTimeoutInSeconds) {
      case QueuePair(queue, dlq) =>
        val client = successfulClient(ingest)

        withIngestWorker(queue, client) { _ =>
          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          Thread.sleep(waitTimeInMilliseconds)

          getMessages(queue) shouldBe empty
          getMessages(dlq) shouldBe empty
        }
    }
  }

  it("When the client conflicts it should consume the message") {
    withLocalSqsQueueAndDlqAndTimeout(visibilityTimeoutInSeconds) {
      case QueuePair(queue, dlq) =>
        val client = conflictClient(ingestStatusUpdate)

        withIngestWorker(queue, client) { _ =>
          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          Thread.sleep(waitTimeInMilliseconds)

          getMessages(queue) shouldBe empty
          getMessages(dlq) shouldBe empty
        }
    }
  }

  it("When the client errors it should NOT consume the message") {
    withLocalSqsQueueAndDlqAndTimeout(visibilityTimeoutInSeconds) {
      case QueuePair(queue, dlq) =>
        val client = unknownErrorClient(ingestStatusUpdate)

        withIngestWorker(queue, client) { _ =>
          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          eventually {
            getMessages(queue) shouldBe empty
            getMessages(dlq).length shouldBe 1
          }
        }
    }
  }

  it("When the client fails it should NOT consume the message") {
    withLocalSqsQueueAndDlqAndTimeout(visibilityTimeoutInSeconds) {
      case QueuePair(queue, dlq) =>
        val client = failedFutureClient

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
