package uk.ac.wellcome.platform.archive.common.operation

import java.util.UUID

import org.scalatest.FunSpec
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.fixtures.{OperationFixtures, RandomThings}
import uk.ac.wellcome.platform.archive.common.generators.OperationGenerators
import uk.ac.wellcome.platform.archive.common.ingest.IngestUpdateAssertions

import scala.concurrent.ExecutionContext.Implicits.global

class OutgoingPublisherTest extends FunSpec
  with RandomThings
  with ScalaFutures
  with IngestUpdateAssertions
  with Eventually
  with IntegrationPatience
  with OperationFixtures
  with OperationGenerators {

  val operationName: String = randomAlphanumeric()

  it("publishes an outgoing message when successful") {
    withLocalSnsTopic { topic =>
      withOutgoingPublisher(operationName, topic) { outgoingPublisher =>
        val requestId = UUID.randomUUID()
        val summary = createTestSummary()

        val sendingOperationNotice = outgoingPublisher.send(requestId, createOperationSuccessWith(summary))(identity)

        whenReady(sendingOperationNotice) { _ =>
          assertSnsReceivesOnly(summary, topic)
        }

      }
    }
  }

  it("publishes an outgoing message when completed") {
    withLocalSnsTopic { topic =>
      withOutgoingPublisher(operationName, topic) { outgoingPublisher =>
        val requestId = UUID.randomUUID()
        val summary = createTestSummary()

        val sendingOperationNotice = outgoingPublisher.send(requestId, createOperationCompletedWith(summary))(identity)

        whenReady(sendingOperationNotice) { _ =>
          assertSnsReceivesOnly(summary, topic)
        }

      }
    }
  }

  it("does not publish an outgoing message when failed") {
    withLocalSnsTopic { topic =>
      withOutgoingPublisher(operationName, topic) { outgoingPublisher =>
        val requestId = UUID.randomUUID()
        val summary = createTestSummary()

        val sendingOperationNotice = outgoingPublisher.send(requestId, createOperationFailureWith(summary))(identity)

        whenReady(sendingOperationNotice) { _ =>
          assertSnsReceivesNothing(topic)
        }
      }
    }
  }

}
