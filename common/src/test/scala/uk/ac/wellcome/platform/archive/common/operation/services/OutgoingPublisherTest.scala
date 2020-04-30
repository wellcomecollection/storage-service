package uk.ac.wellcome.platform.archive.common.operation.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.prop.TableDrivenPropertyChecks._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.SourceLocationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{
  IngestOperationGenerators,
  PayloadGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

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
