package uk.ac.wellcome.platform.archive.common.operation

import java.util.UUID

import org.scalatest.FunSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models.Progress
import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.ExecutionContext.Implicits.global

class OperationNotifierTest
    extends FunSpec
    with RandomThings
    with ScalaFutures
    with ProgressUpdateAssertions
    with Eventually
    with IntegrationPatience
    with SNS {

  case class TestSummary(
    description: String
  ) {
    override def toString: String = this.description
  }

  describe("with a failed operation") {
    it("only sends a failed status to progressTopic") {
      withLocalSnsTopic { progressTopic =>
        withSNSWriter(progressTopic) { progressSnsWriter =>
          withLocalSnsTopic { outgoingTopic =>
            withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
              val requestId = UUID.randomUUID()

              val operationName = randomAlphanumeric()
              val operationNotifier = new OperationNotifier(
                operationName,
                outgoingSnsWriter,
                progressSnsWriter
              )

              val summary = TestSummary(
                randomAlphanumeric()
              )

              val throwable =
                new RuntimeException(
                  randomAlphanumeric()
                )

              val operation = OperationFailure(
                summary,
                throwable
              )

              val sendingOperationNotice =
                operationNotifier
                  .send(requestId, operation)(identity)

              whenReady(sendingOperationNotice) { _ =>
                eventually {

                  assertTopicReceivesProgressStatusUpdate(
                    requestId = requestId,
                    progressTopic = progressTopic,
                    status = Progress.Failed
                  ) { events =>
                    val description = events.map {
                      _.description
                    }.head

                    description should startWith(
                      s"${operationName.capitalize} failed"
                    )
                  }

                  assertSnsReceivesNothing(outgoingTopic)
                }
              }
            }
          }
        }
      }
    }
  }

  describe("with a successful operation") {
    it("sends an event update to progressTopic and a message to outgoingTopic") {
      withLocalSnsTopic { progressTopic =>
        withSNSWriter(progressTopic) { progressSnsWriter =>
          withLocalSnsTopic { outgoingTopic =>
            withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
              val requestId = UUID.randomUUID()

              val operationName = randomAlphanumeric()
              val operationNotifier = new OperationNotifier(
                operationName,
                outgoingSnsWriter,
                progressSnsWriter
              )

              val summary = TestSummary(
                randomAlphanumeric()
              )

              val operation = OperationSuccess(
                summary
              )

              val sendingOperationNotice =
                operationNotifier
                  .send(requestId, operation)(identity)

              whenReady(sendingOperationNotice) { _ =>
                eventually {

                  assertTopicReceivesProgressEventUpdate(
                    requestId,
                    progressTopic) { events =>
                    events should have size 1
                    events.head.description shouldBe s"${operationName.capitalize} succeeded"
                  }

                  assertSnsReceivesOnly(summary, outgoingTopic)
                }
              }
            }
          }
        }
      }
    }
  }

  describe("with a completed operation") {
    it(
      "sends a completed status update to progressTopic and a message to outgoingTopic") {
      withLocalSnsTopic { progressTopic =>
        withSNSWriter(progressTopic) { progressSnsWriter =>
          withLocalSnsTopic { outgoingTopic =>
            withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
              val requestId = UUID.randomUUID()

              val operationName = randomAlphanumeric()
              val operationNotifier = new OperationNotifier(
                operationName,
                outgoingSnsWriter,
                progressSnsWriter
              )

              val summary = TestSummary(
                randomAlphanumeric()
              )

              val operation = OperationCompleted(
                summary
              )

              val sendingOperationNotice =
                operationNotifier
                  .send(requestId, operation)(identity)

              whenReady(sendingOperationNotice) { _ =>
                eventually {
                  assertTopicReceivesProgressStatusUpdate(
                    requestId = requestId,
                    progressTopic = progressTopic,
                    status = Progress.Completed
                  ) { events =>
                    val description = events.map {
                      _.description
                    }.head

                    description should startWith(
                      s"${operationName.capitalize} succeeded (completed)"
                    )
                  }

                  assertSnsReceivesOnly(summary, outgoingTopic)
                }
              }
            }
          }
        }
      }
    }
  }
}
