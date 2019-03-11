package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.operation.{IngestNotifier, OperationNotifier, OutgoingNotifier}

trait OperationFixtures extends SNS {
  def withIngestNotifier[R](operationName: String, topic: Topic)(testWith: TestWith[IngestNotifier, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val ingestNotifier = new IngestNotifier(operationName, snsWriter)

      testWith(ingestNotifier)
    }

  def withOutgoingNotifier[R](operationName: String, topic: Topic)(testWith: TestWith[OutgoingNotifier, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val outgoingNotifier = new OutgoingNotifier(operationName, snsWriter)

      testWith(outgoingNotifier)
    }

  def withOperationNotifier[R](operationName: String, ingestTopic: Topic, outgoingTopic: Topic)(testWith: TestWith[OperationNotifier, R]): R =
    withIngestNotifier(operationName, ingestTopic) { ingests =>
      withOutgoingNotifier(operationName, outgoingTopic) { outgoing =>
        val operationNotifier = new OperationNotifier(outgoing, ingests)

        testWith(operationNotifier)
      }
    }
}
