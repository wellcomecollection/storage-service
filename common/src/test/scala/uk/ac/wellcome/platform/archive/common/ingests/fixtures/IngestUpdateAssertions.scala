package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import grizzled.slf4j.Logging
import org.scalatest.{Assertion, Inside, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion._
import uk.ac.wellcome.platform.archive.common.ingests.models._

import scala.util.Try

trait IngestUpdateAssertions extends Inside with Logging with Matchers {
  def assertTopicReceivesIngestStatus[R](
    ingestId: IngestID,
    ingests: MemoryMessageSender,
    status: Ingest.Status
  )(assert: Seq[IngestEvent] => R): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingests) { ingestUpdates =>
      ingestUpdates.size should be > 0

      val (success, failures) = ingestUpdates
        .map { ingestUpdate =>
          debug(s"Received IngestUpdate: $ingestUpdate")
          Try(inside(ingestUpdate) {
            case IngestStatusUpdate(id, actualStatus, events) =>
              id shouldBe ingestId
              actualStatus shouldBe status
              assert(events)
          })
        }
        .partition(_.isSuccess)

      if (success.size != 1) {
        debug(s"Failures: $failures")
      }

      success should have size 1
    }

  def assertTopicReceivesIngestUpdates(
    ingestId: IngestID,
    ingests: MemoryMessageSender
  )(assert: Seq[IngestUpdate] => Assertion): Assertion =
    assert(ingests.getMessages[IngestUpdate])

  def assertTopicReceivesIngestEvents(
    ingestId: IngestID,
    ingests: MemoryMessageSender,
    expectedDescriptions: Seq[String]
  ): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingests) {
      ingestUpdates: Seq[IngestUpdate] =>

      val eventDescriptions: Seq[String] =
        ingestUpdates
          .flatMap { _.events }
          .map { _.description }
          .distinct

      eventDescriptions should contain allElementsOf expectedDescriptions.distinct
    }

  def assertTopicReceivesIngestEvent(
    ingestId: IngestID,
    ingests: MemoryMessageSender
  )(assert: Seq[IngestEvent] => Assertion): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingests) { ingestUpdates =>
      ingestUpdates.size should be > 0

      val (success, _) = ingestUpdates
        .map { ingestUpdate =>
          debug(s"Received IngestUpdate: $ingestUpdate")
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
