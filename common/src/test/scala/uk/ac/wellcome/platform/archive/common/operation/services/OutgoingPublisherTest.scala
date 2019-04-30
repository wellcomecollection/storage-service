package uk.ac.wellcome.platform.archive.common.operation.services

import org.scalatest.FunSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.prop.TableDrivenPropertyChecks._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{
  IngestOperationGenerators,
  PayloadGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

import scala.concurrent.ExecutionContext.Implicits.global

class OutgoingPublisherTest
    extends FunSpec
    with ScalaFutures
    with IngestUpdateAssertions
    with Eventually
    with IntegrationPatience
    with OperationFixtures
    with IngestOperationGenerators
    with PayloadGenerators {

  val operationName: String = randomAlphanumeric()

  it("sends outgoing message if operation is successful") {
    val successfulOperations =
      Table("operation", createOperationSuccess(), createOperationCompleted())
    forAll(successfulOperations) { operation =>
      withLocalSnsTopic { topic =>
        withOutgoingPublisher(operationName, topic) { outgoingPublisher =>
          val outgoing = createObjectLocationPayload

          val sendingOperationNotice =
            outgoingPublisher.sendIfSuccessful(operation, outgoing)

          whenReady(sendingOperationNotice) { _ =>
            assertSnsReceivesOnly(outgoing, topic)
          }
        }
      }
    }
  }

  it("does not send outgoing if operation failed") {
    withLocalSnsTopic { topic =>
      withOutgoingPublisher(operationName, topic) { outgoingPublisher =>
        val outgoing = createObjectLocationPayload

        val sendingOperationNotice =
          outgoingPublisher.sendIfSuccessful(createOperationFailure(), outgoing)

        whenReady(sendingOperationNotice) { _ =>
          assertSnsReceivesNothing(topic)
        }
      }
    }
  }
}
