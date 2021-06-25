package weco.storage_service.operation.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.prop.TableDrivenPropertyChecks._
import weco.json.JsonUtil._
import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.SourceLocationPayload
import weco.storage_service.fixtures.OperationFixtures
import weco.storage_service.generators.{
  IngestOperationGenerators,
  PayloadGenerators
}
import weco.storage_service.ingests.fixtures.IngestUpdateAssertions

import scala.util.Success

class OutgoingPublisherTest
    extends AnyFunSpec
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
      val outgoing = createSourceLocationPayload

      val notice = outgoingPublisher.sendIfSuccessful(operation, outgoing)

      notice shouldBe a[Success[_]]

      messageSender.getMessages[SourceLocationPayload] shouldBe Seq(outgoing)
    }
  }

  it("does not send outgoing if operation failed") {
    val messageSender = new MemoryMessageSender()
    val outgoingPublisher = createOutgoingPublisherWith(messageSender)
    val outgoing = createSourceLocationPayload

    val notice =
      outgoingPublisher.sendIfSuccessful(createOperationFailure(), outgoing)

    notice shouldBe a[Success[_]]

    messageSender.messages shouldBe empty
  }
}
