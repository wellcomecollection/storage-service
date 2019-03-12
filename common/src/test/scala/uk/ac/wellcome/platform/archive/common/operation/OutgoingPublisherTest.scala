package uk.ac.wellcome.platform.archive.common.operation

import org.scalatest.FunSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.fixtures.{
  OperationFixtures,
  RandomThings
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagRequestGenerators,
  OperationGenerators
}
import uk.ac.wellcome.platform.archive.common.ingest.IngestUpdateAssertions

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.prop.TableDrivenPropertyChecks._

class OutgoingPublisherTest
    extends FunSpec
    with RandomThings
    with ScalaFutures
    with IngestUpdateAssertions
    with Eventually
    with IntegrationPatience
    with OperationFixtures
    with OperationGenerators
    with BagRequestGenerators {

  val operationName: String = randomAlphanumeric()

  it("sends outgoing message if operation is successful") {
    val successfulOperations =
      Table("operation", createOperationSuccess(), createOperationCompleted())
    forAll(successfulOperations) { operation =>
      withLocalSnsTopic { topic =>
        withOutgoingPublisher(operationName, topic) { outgoingPublisher =>
          val outgoing = createBagRequest()

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
        val outgoing = createBagRequest()

        val sendingOperationNotice =
          outgoingPublisher.sendIfSuccessful(createOperationFailure(), outgoing)

        whenReady(sendingOperationNotice) { _ =>
          assertSnsReceivesNothing(topic)
        }
      }
    }
  }
}
