package uk.ac.wellcome.platform.archive.common.ingests.services

import org.scalatest.FunSpec
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  IngestOperationGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepStarted

import scala.util.Success

class IngestUpdaterTest
    extends FunSpec
    with IngestUpdateAssertions
    with OperationFixtures
    with IngestOperationGenerators
    with BagIdGenerators {

  val stepName: String = randomAlphanumericWithLength()

  val ingestId: IngestID = createIngestID
  val summary: TestSummary = createTestSummary()

  it("sends an ingest update when successful") {
    val messageSender = new MemoryMessageSender()
    val ingestUpdater = createIngestUpdaterWith(
      stepName = stepName,
      messageSender = messageSender
    )

    val update =
      ingestUpdater.send(ingestId, createOperationSuccessWith(summary))

    update shouldBe a[Success[_]]

    assertTopicReceivesIngestEvent(ingestId, messageSender) { events =>
      events should have size 1
      events.head.description shouldBe s"${stepName.capitalize} succeeded"
    }
  }

  it("sends an ingest update when completed") {
    val messageSender = new MemoryMessageSender()
    val ingestUpdater = createIngestUpdaterWith(
      stepName = stepName,
      messageSender = messageSender
    )

    val update = ingestUpdater.send(
      ingestId = ingestId,
      step = createOperationCompletedWith(summary)
    )

    update shouldBe a[Success[_]]

    assertTopicReceivesIngestStatus(
      ingestId = ingestId,
      ingests = messageSender,
      status = Ingest.Succeeded
    ) { events =>
      events should have size 1
      events.head.description shouldBe s"${stepName.capitalize} succeeded (completed)"
    }
  }

  it("sends an ingest update when failed") {
    val messageSender = new MemoryMessageSender()
    val ingestUpdater = createIngestUpdaterWith(
      stepName = stepName,
      messageSender = messageSender
    )

    val update = ingestUpdater.send(
      ingestId = ingestId,
      step = createIngestFailureWith(summary)
    )

    update shouldBe a[Success[_]]

    assertTopicReceivesIngestStatus(
      ingestId = ingestId,
      ingests = messageSender,
      status = Ingest.Failed
    ) { events =>
      events should have size 1
      events.head.description shouldBe s"${stepName.capitalize} failed"
    }
  }

  it("sends an ingest update when an ingest step starts") {
    val messageSender = new MemoryMessageSender()
    val ingestUpdater = createIngestUpdaterWith(
      stepName = stepName,
      messageSender = messageSender
    )

    val update = ingestUpdater.send(
      ingestId = ingestId,
      step = IngestStepStarted(ingestId)
    )

    update shouldBe a[Success[_]]

    assertTopicReceivesIngestEvent(ingestId, messageSender) { events =>
      events should have size 1
      events.head.description shouldBe s"${stepName.capitalize} started"
    }
  }

  it("sends an ingest update when failed with a failure message") {
    val messageSender = new MemoryMessageSender()
    val ingestUpdater = createIngestUpdaterWith(
      stepName = stepName,
      messageSender = messageSender
    )

    val failureMessage = randomAlphanumericWithLength(length = 50)

    val update = ingestUpdater.send(
      ingestId = ingestId,
      step = createIngestFailureWith(
        summary,
        maybeFailureMessage = Some(failureMessage)
      )
    )

    update shouldBe a[Success[_]]

    assertTopicReceivesIngestStatus(
      ingestId = ingestId,
      ingests = messageSender,
      status = Ingest.Failed
    ) { events =>
      events should have size 1
      events.head.description shouldBe s"${stepName.capitalize} failed - $failureMessage"
    }
  }
}
