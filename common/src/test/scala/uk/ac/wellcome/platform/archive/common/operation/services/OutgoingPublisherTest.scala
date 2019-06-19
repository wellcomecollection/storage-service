package uk.ac.wellcome.platform.archive.common.operation.services

import io.circe.Json
import io.circe.syntax._
import org.scalatest.FunSpec
import org.scalatest.prop.TableDrivenPropertyChecks._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{IngestOperationGenerators, PayloadGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

import scala.util.Success

class OutgoingPublisherTest
    extends FunSpec
    with IngestUpdateAssertions
    with OperationFixtures
    with IngestOperationGenerators
    with PayloadGenerators {

  it("sends outgoing message if operation is successful") {
    val successfulOperations =
      Table("operation", createOperationSuccess(), createOperationCompleted())
    forAll(successfulOperations) { operation =>
      val messageSender = new MemoryMessageSender()
      val outgoingPublisher = createOutgoingPublisherWith(messageSender)

      val json = Json.obj(
        ("color", "red".asJson),
        ("number", 5.asJson)
      )

      val notice = outgoingPublisher.sendIfSuccessful(operation, json)

      notice shouldBe a[Success[_]]

      messageSender.getMessages[Json] shouldBe Seq(json)
    }
  }

  it("does not send outgoing if operation failed") {
    val messageSender = new MemoryMessageSender()
    val outgoingPublisher = createOutgoingPublisherWith(messageSender)

    val json = Json.obj(
      ("color", "red".asJson),
      ("number", 5.asJson)
    )

    val notice =
      outgoingPublisher.sendIfSuccessful(createOperationFailure(), json)

    notice shouldBe a[Success[_]]

    messageSender.messages shouldBe empty
  }
}
