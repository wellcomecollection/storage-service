package uk.ac.wellcome.platform.storage.ingests_worker.services

import java.time.Instant

import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.{GetQueueAttributesRequest, QueueAttributeName}
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.messaging.worker.models.{NonDeterministicFailure, Successful}
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Succeeded
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestUpdate}
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{IngestTrackerClient, IngestTrackerError}
import uk.ac.wellcome.platform.storage.ingests_worker.fixtures.IngestsWorkerFixtures

import scala.concurrent.Future

class IngestsWorkerServiceTest
  extends AnyFunSpec
    with Matchers
    with Eventually
    with HttpFixtures
    with ScalaFutures
    with IngestsWorkerFixtures
    with IngestGenerators
    with IntegrationPatience {

  val visibilityTimeout = 5

  val ingest = createIngestWith(
    createdDate = Instant.now()
  )

  val ingestStatusUpdate =
    createIngestStatusUpdateWith(
      id = ingest.id,
      status = Succeeded
    )

  describe("When the client succeeds") {
    val client = new IngestTrackerClient {
      override def updateIngest(ingestUpdate: IngestUpdate): Future[Either[IngestTrackerError, Ingest]] =
        Future.successful(Right(ingest))
    }

    it("processes the message") {
      withIngestWorker(ingestTrackerClient = client) { worker =>
        withLocalSqsQueueAndDlqAndTimeout(visibilityTimeout) {
          case QueuePair(queue, _) =>
            val someWork = worker.processMessage(ingestStatusUpdate)

            whenReady(someWork) { result =>
              result shouldBe a[Successful[_]]
              getMessages(queue) shouldBe empty
            }
        }
      }
    }
  }

  // TODO: Test retryable and non-retryable failures
  describe("When the client fails") {
    val client = new IngestTrackerClient {
      override def updateIngest(ingestUpdate: IngestUpdate): Future[Either[IngestTrackerError, Ingest]] =
        Future.failed(new Exception("BOOM!"))
    }

    it("does not process the message") {
      withLocalSqsQueue { queue =>

          withIngestWorker(queue, ingestTrackerClient = client) { worker =>

            val someWork = worker.processMessage(ingestStatusUpdate)

            whenReady(someWork) { result =>
              result shouldBe a[NonDeterministicFailure[_]]

              val waitTime = (visibilityTimeout * 1000) + 1000
              Thread.sleep(waitTime)

              eventually {
                val queueAttributes = sqsClient
                  .getQueueAttributes {
                    builder: GetQueueAttributesRequest.Builder =>
                      builder
                        .queueUrl(queue.url)
                        .attributeNames(
                          QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                          QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES
                        )
                  }
                  .attributes()

                queueAttributes.get(
                  QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE
                ) shouldBe "1"
                queueAttributes.get(
                  QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES
                ) shouldBe "0"
              }
            }
        }
      }
    }
  }
}
