package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import grizzled.slf4j.Logging
import org.scalatest.{Assertion, Inside, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._

import scala.util.Try

trait IngestUpdateAssertions extends Inside with Logging { this: Matchers =>
  def assertReceivesIngestUpdates(messageSender: MemoryMessageSender)(ingestId: IngestID)(assert: Seq[IngestUpdate] => Assertion): Assertion = {
    val ingestUpdates = messageSender.getMessages[IngestUpdate]()

    assert(ingestUpdates)
  }

  def assertReceivesIngestStatus[R](messageSender: MemoryMessageSender)(
    ingestId: IngestID,
    status: Ingest.Status,
    expectedBag: Option[BagId] = None
  )(
    assert: Seq[IngestEvent] => R
  ): Assertion =
    assertReceivesIngestUpdates(messageSender)(ingestId) { ingestUpdates =>
      ingestUpdates.size should be > 0

      val (success, failures) = ingestUpdates
        .map { ingestUpdate =>
          debug(s"Received IngestUpdate: $ingestUpdate")
          Try(inside(ingestUpdate) {
            case IngestStatusUpdate(id, actualStatus, maybeBag, events) =>
              id shouldBe ingestId
              actualStatus shouldBe status
              maybeBag shouldBe expectedBag
              assert(events)
          })
        }
        .partition(_.isSuccess)

      if (success.size != 1) {
        println(s"Failures: $failures")
      }

      success should have size 1
    }

  def assertReceivesIngestEvents(
    messageSender: MemoryMessageSender
  )(
    ingestId: IngestID,
    expectedDescriptions: Seq[String]
  ): Assertion =
    assertReceivesIngestUpdates(messageSender)(ingestId) { ingestUpdates =>
      val eventDescriptions: Seq[String] =
        ingestUpdates
          .flatMap { _.events }
          .map { _.description }
          .distinct

      eventDescriptions should contain theSameElementsAs expectedDescriptions
    }

  def assertReceivesIngestEvent(
    messageSender: MemoryMessageSender
  )(
    ingestId: IngestID
  )(
    assert: Seq[IngestEvent] => Assertion
  ): Assertion =
    assertReceivesIngestUpdates(messageSender)(ingestId) { ingestUpdates =>
      ingestUpdates.size should be > 0

      val (success, _) = ingestUpdates
        .map { ingestUpdate =>
          println(s"Received IngestUpdate: $ingestUpdate")
          Try(inside(ingestUpdate) {
            case IngestEventUpdate(id, events) =>
              id shouldBe ingestId

              assert(events)
          })
        }
        .partition(_.isSuccess)

      success.distinct should have size 1
    }
}
