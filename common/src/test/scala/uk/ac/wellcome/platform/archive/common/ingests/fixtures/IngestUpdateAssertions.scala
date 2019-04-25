package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import grizzled.slf4j.Logging
import org.scalatest.{Assertion, Inside}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._

import scala.util.Try

trait IngestUpdateAssertions extends SNS with Inside with Logging {
  def assertTopicReceivesIngestStatus[R](ingestId: IngestID,
                                         ingestTopic: SNS.Topic,
                                         status: Ingest.Status,
                                         expectedBag: Option[BagId] = None)(
    assert: Seq[IngestEvent] => R): Assertion = {
    val messages = listMessagesReceivedFromSNS(ingestTopic)
    val ingestUpdates = messages.map { messageinfo =>
      fromJson[IngestUpdate](messageinfo.message).get
    }.distinct

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

  def assertTopicReceivesIngestEvent(ingestId: IngestID, ingestTopic: SNS.Topic)(
    assert: Seq[IngestEvent] => Assertion): Assertion = {

    val messages = listMessagesReceivedFromSNS(ingestTopic)

    val ingestUpdates = messages.map { messageinfo =>
      fromJson[IngestUpdate](messageinfo.message).get
    }

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
