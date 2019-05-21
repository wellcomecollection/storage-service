package uk.ac.wellcome.platform.archive.common.ingests.services

import org.scalatest.FunSpec
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{BagIdGenerators, IngestOperationGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepStarted

import scala.util.Success

class IngestUpdaterTest
    extends FunSpec
    with IngestUpdateAssertions
    with OperationFixtures
    with IngestOperationGenerators
    with BagIdGenerators {

  val stepName: String = createStepName
  val ingestId: IngestID = createIngestID
  val summary: TestSummary = createTestSummary()
  val bagId: BagId = createBagId

  it("sends an ingest update when successful") {
    val messageSender = createMessageSender

    val ingestUpdater = createIngestUpdater(
      stepName = stepName,
      messageSender = messageSender
    )

    val sendingOperationNotice =
      ingestUpdater.send(ingestId, createOperationSuccessWith(summary))

    sendingOperationNotice shouldBe Success(())

    println(messageSender.messages)
    true shouldBe false

//          eventually {
//            assertTopicReceivesIngestEvent(ingestId, ingestTopic) { events =>
//              events should have size 1
//              events.head.description shouldBe s"${stepName.capitalize} succeeded"
//            }
//          }
//        }
//
//      }
//    }
  }

  it("sends an ingest update when completed") {
    val messageSender = createMessageSender

    val ingestUpdater = createIngestUpdater(
      stepName = stepName,
      messageSender = messageSender
    )

    val sendingOperationNotice = ingestUpdater.send(
      ingestId = ingestId,
      step = createOperationCompletedWith(summary),
      bagId = Some(bagId)
    )

    sendingOperationNotice shouldBe Success(())

    println(messageSender.messages)
    true shouldBe false

//        whenReady(sendingOperationNotice) { _ =>
//          eventually {
//            assertTopicReceivesIngestStatus(
//              ingestId,
//              ingestTopic,
//              Ingest.Completed,
//              Some(bagId)) { events =>
//              events should have size 1
//              events.head.description shouldBe s"${stepName.capitalize} succeeded (completed)"
//            }
//          }
//        }
//      }
//    }
  }

  it("sends an ingest update when failed") {
    val messageSender = createMessageSender

    val ingestUpdater = createIngestUpdater(
      stepName = stepName,
      messageSender = messageSender
    )

    val sendingOperationNotice = ingestUpdater.send(
      ingestId = ingestId,
      step = createIngestFailureWith(summary),
      bagId = Some(bagId)
    )

    sendingOperationNotice shouldBe Success(())

    println(messageSender.messages)
    true shouldBe false

//        whenReady(sendingOperationNotice) { _ =>
//          eventually {
//            assertTopicReceivesIngestStatus(
//              ingestId,
//              ingestTopic,
//              Ingest.Failed,
//              Some(bagId)) { events =>
//              events should have size 1
//              events.head.description shouldBe s"${stepName.capitalize} failed"
//            }
//          }
//        }
//      }
//    }
  }

  it("sends an ingest update when an ingest step starts") {
    val messageSender = createMessageSender

    val ingestUpdater = createIngestUpdater(
      stepName = stepName,
      messageSender = messageSender
    )

    val sendingOperationNotice = ingestUpdater.send(
      ingestId = ingestId,
      step = IngestStepStarted(ingestId)
    )

    sendingOperationNotice shouldBe Success(())

    println(messageSender.messages)
    true shouldBe false

//        whenReady(sendingOperationNotice) { _ =>
//          eventually {
//            assertTopicReceivesIngestEvent(ingestId, ingestTopic) { events =>
//              events should have size 1
//              events.head.description shouldBe s"${stepName.capitalize} started"
//            }
//          }
//        }
//      }
//    }
  }

  it("sends an ingest update when failed with a failure message") {
    val messageSender = createMessageSender

    val ingestUpdater = createIngestUpdater(
      stepName = stepName,
      messageSender = messageSender
    )

    val failureMessage = randomAlphanumeric(length = 50)

    val sendingOperationNotice = ingestUpdater.send(
      ingestId = ingestId,
      step = createIngestFailureWith(
        summary,
        maybeFailureMessage = Some(failureMessage)
      ),
      bagId = Some(bagId)
    )

    sendingOperationNotice shouldBe Success(())

    println(messageSender.messages)
    true shouldBe false

//        whenReady(sendingOperationNotice) { _ =>
//          eventually {
//            assertTopicReceivesIngestStatus(
//              ingestId,
//              ingestTopic,
//              Ingest.Failed,
//              Some(bagId)) { events =>
//              events should have size 1
//              events.head.description shouldBe s"${stepName.capitalize} failed - $failureMessage"
//            }
//          }
//        }
//      }
//    }
  }
}
