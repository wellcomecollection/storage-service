package uk.ac.wellcome.platform.archive.common.operation.services

import org.scalatest.FunSpec
import org.scalatest.prop.TableDrivenPropertyChecks._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{IngestOperationGenerators, PayloadGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

import scala.util.{Success, Try}

class OutgoingPublisherTest
    extends FunSpec
    with IngestUpdateAssertions
    with OperationFixtures
    with IngestOperationGenerators
    with PayloadGenerators {

  val operationName: String = randomAlphanumeric()

  it("sends outgoing message if operation is successful") {
    val successfulOperations =
      Table("operation", createOperationSuccess(), createOperationCompleted())
    forAll(successfulOperations) { operation =>
      val messageSender = createMessageSender
      val outgoingPublisher = createOutgoingPublisher(
        messageSender = messageSender
      )

      val outgoing = createIngestRequestPayload

      val sendingOperationNotice: Try[Unit] =
        outgoingPublisher.sendIfSuccessful(operation, outgoing)

      sendingOperationNotice shouldBe Success(())

      println(messageSender.messages)
      true shouldBe false

//          whenReady(sendingOperationNotice) { _ =>
//            assertSnsReceivesOnly(outgoing, topic)
//          }
//        }
//      }
    }
  }

  it("does not send outgoing if operation failed") {
    val messageSender = createMessageSender
    val outgoingPublisher = createOutgoingPublisher(
      messageSender = messageSender
    )

    val outgoing = createIngestRequestPayload

    val sendingOperationNotice =
      outgoingPublisher.sendIfSuccessful(createOperationFailure(), outgoing)

    sendingOperationNotice shouldBe Success(())

    println(messageSender.messages)
    true shouldBe false

//    withLocalSnsTopic { topic =>
//      withOutgoingPublisher(operationName, topic) { outgoingPublisher =>
//        val outgoing = createIngestRequestPayload
//
//        val sendingOperationNotice =
//          outgoingPublisher.sendIfSuccessful(createOperationFailure(), outgoing)
//
//        whenReady(sendingOperationNotice) { _ =>
//          assertSnsReceivesNothing(topic)
//        }
//      }
//    }
  }
}
