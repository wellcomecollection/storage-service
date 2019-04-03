package uk.ac.wellcome.platform.archive.common.ingests.services

import java.util.UUID

import org.scalatest.FunSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  OperationFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  IngestOperationGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

class IngestUpdaterTest
    extends FunSpec
    with RandomThings
    with ScalaFutures
    with IngestUpdateAssertions
    with Eventually
    with IntegrationPatience
    with OperationFixtures
    with IngestOperationGenerators
    with BagIdGenerators {

  val stepName: String = randomAlphanumeric()

  it("sends an ingest update when successful") {
    withLocalSnsTopic { ingestTopic =>
      withIngestUpdater(stepName, ingestTopic) { ingestUpdater =>
        val requestId = UUID.randomUUID()
        val summary = createTestSummary()

        val sendingOperationNotice =
          ingestUpdater.send(requestId, createOperationSuccessWith(summary))

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestEvent(requestId, ingestTopic) { events =>
              events should have size 1
              events.head.description shouldBe s"${stepName.capitalize} succeeded"
            }
          }
        }

      }
    }
  }

  it("sends an ingest update when completed") {
    withLocalSnsTopic { ingestTopic =>
      withIngestUpdater(stepName, ingestTopic) { ingestUpdater =>
        val requestId = UUID.randomUUID()
        val summary = createTestSummary()

        val bagId = createBagId
        val sendingOperationNotice = ingestUpdater.send(
          requestId,
          createOperationCompletedWith(summary),
          Some(bagId))

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestStatus(
              requestId,
              ingestTopic,
              Ingest.Completed,
              Some(bagId)) { events =>
              events should have size 1
              events.head.description shouldBe s"${stepName.capitalize} succeeded (completed)"
            }
          }
        }
      }
    }
  }

  it("sends an ingest update when failed") {
    withLocalSnsTopic { ingestTopic =>
      withIngestUpdater(stepName, ingestTopic) { ingestUpdater =>
        val requestId = UUID.randomUUID()
        val summary = createTestSummary()

        val bagId = createBagId
        val sendingOperationNotice = ingestUpdater.send(
          requestId,
          createIngestFailureWith(summary),
          Some(bagId))

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestStatus(
              requestId,
              ingestTopic,
              Ingest.Failed,
              Some(bagId)) { events =>
              events should have size 1
              events.head.description shouldBe s"${stepName.capitalize} failed"
            }
          }
        }
      }
    }
  }

  it("sends an ingest update when failed with a failure message") {
    withLocalSnsTopic { ingestTopic =>
      withIngestUpdater(stepName, ingestTopic) { ingestUpdater =>
        val requestId = UUID.randomUUID()
        val summary = createTestSummary()
        val failureMessage = randomAlphanumeric(length = 50)

        val bagId = createBagId
        val sendingOperationNotice = ingestUpdater.send(
          requestId,
          createIngestFailureWith(summary, maybeFailureMessage = Some(failureMessage)),
          Some(bagId))

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestStatus(
              requestId,
              ingestTopic,
              Ingest.Failed,
              Some(bagId)) { events =>
              events should have size 1
              events.head.description shouldBe s"${stepName.capitalize} failed - $failureMessage"
            }
          }
        }
      }
    }
  }
}
