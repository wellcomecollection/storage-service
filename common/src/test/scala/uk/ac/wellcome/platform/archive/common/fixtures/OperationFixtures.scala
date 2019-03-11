package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.operation.{IngestUpdater, OperationNotifier, DiagnosticReporter, OutgoingPublisher}

trait OperationFixtures extends SNS with MetricsSenderFixtures {
  def withIngestNotifier[R](operationName: String, topic: Topic)(testWith: TestWith[IngestUpdater, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val ingestNotifier = new IngestUpdater(operationName, snsWriter)

      testWith(ingestNotifier)
    }

  def withOutgoingNotifier[R](operationName: String, topic: Topic)(testWith: TestWith[OutgoingPublisher, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val outgoingNotifier = new OutgoingPublisher(operationName, snsWriter)

      testWith(outgoingNotifier)
    }

  def withOperationNotifier[R](
    operationName: String,
    ingestTopic: Topic,
    outgoingTopic: Topic)(testWith: TestWith[OperationNotifier, R]): R =
    withIngestNotifier(operationName, ingestTopic) { ingests =>
      withOutgoingNotifier(operationName, outgoingTopic) { outgoing =>
        val operationNotifier = new OperationNotifier(outgoing, ingests)

        testWith(operationNotifier)
      }
    }

  def withOperationReporter[R]()(testWith: TestWith[DiagnosticReporter, R]): R =
    withMetricsSender { metricsSender =>
      val reporter = new DiagnosticReporter(metricsSender)

      testWith(reporter)
    }
}
