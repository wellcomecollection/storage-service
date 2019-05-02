package uk.ac.wellcome.platform.archive.common.ingests.services

import org.scalatest.FunSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{BagIdGenerators, IngestOperationGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepStarted

class IngestUpdaterTest
    extends FunSpec
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
        val ingestId = createIngestID
        val summary = createTestSummary()

        val sendingOperationNotice =
          ingestUpdater.send(ingestId, createOperationSuccessWith(summary))

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestEvent(ingestId, ingestTopic) { events =>
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
        val ingestId = createIngestID
        val summary = createTestSummary()

        val bagId = createBagId
        val sendingOperationNotice = ingestUpdater.send(
          ingestId = ingestId,
          step = createOperationCompletedWith(summary),
          bagId = Some(bagId)
        )

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestStatus(
              ingestId,
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
        val ingestId = createIngestID
        val summary = createTestSummary()

        val bagId = createBagId
        val sendingOperationNotice = ingestUpdater.send(
          ingestId = ingestId,
          step = createIngestFailureWith(summary),
          bagId = Some(bagId)
        )

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestStatus(
              ingestId,
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

  it("sends an ingest update when an ingest step starts") {
    withLocalSnsTopic { ingestTopic =>
      withIngestUpdater(stepName, ingestTopic) { ingestUpdater =>
        val ingestId = createIngestID

        val sendingOperationNotice = ingestUpdater.send(
          ingestId = ingestId,
          step = IngestStepStarted(ingestId)
        )

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestEvent(ingestId, ingestTopic) { events =>
              events should have size 1
              events.head.description shouldBe s"${stepName.capitalize} started"
            }
          }
        }
      }
    }
  }

  it("sends an ingest update when failed with a failure message") {
    withLocalSnsTopic { ingestTopic =>
      withIngestUpdater(stepName, ingestTopic) { ingestUpdater =>
        val ingestId = createIngestID
        val summary = createTestSummary()
        val failureMessage = randomAlphanumeric(length = 50)

        val bagId = createBagId
        val sendingOperationNotice = ingestUpdater.send(
          ingestId = ingestId,
          step = createIngestFailureWith(
            summary,
            maybeFailureMessage = Some(failureMessage)
          ),
          bagId = Some(bagId)
        )

        whenReady(sendingOperationNotice) { _ =>
          eventually {
            assertTopicReceivesIngestStatus(
              ingestId,
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
