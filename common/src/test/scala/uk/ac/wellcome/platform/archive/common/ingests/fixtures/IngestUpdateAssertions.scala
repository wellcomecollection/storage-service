package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import grizzled.slf4j.Logging
import org.scalatest.{Assertion, Inside}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._

import scala.util.Try

trait IngestUpdateAssertions extends SNS with Inside with Logging {
  def assertTopicReceivesIngestStatus[R](ingestId: IngestID,
                                         ingestTopic: SNS.Topic,
                                         status: Ingest.Status,
                                         expectedBag: Option[BagId] = None)(
    assert: Seq[IngestEvent] => R): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingestTopic) { ingestUpdates =>
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

  def assertTopicReceivesIngestUpdates(
    ingestId: IngestID,
    ingestTopic: Topic,
  )(assert: Seq[IngestUpdate] => Assertion): Assertion = {
    val ingestUpdates: Seq[IngestUpdate] =
      listNotifications[IngestUpdate](ingestTopic).map { _.get }.distinct

    assert(ingestUpdates)
  }

  def assertTopicReceivesIngestEvents(
    ingestId: IngestID,
    ingestTopic: Topic,
    expectedDescriptions: Seq[String]
  ): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingestTopic) { ingestUpdates =>
      val eventDescriptions: Seq[String] =
        ingestUpdates
          .flatMap { _.events }
          .map { _.description }

      eventDescriptions should contain theSameElementsAs expectedDescriptions
    }

  def assertTopicReceivesIngestEvent(
    ingestId: IngestID,
    ingestTopic: SNS.Topic)(assert: Seq[IngestEvent] => Assertion): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingestTopic) { ingestUpdates =>
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
