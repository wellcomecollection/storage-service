package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.{
  DiagnosticReporter,
  OutgoingPublisher
}

trait OperationFixtures extends SNS with MetricsSenderFixtures {
  def withIngestUpdater[R](operationName: String, topic: Topic)(
    testWith: TestWith[IngestUpdater, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val ingestNotifier = new IngestUpdater(operationName, snsWriter)

      testWith(ingestNotifier)
    }

  def withOutgoingPublisher[R](operationName: String, topic: Topic)(
    testWith: TestWith[OutgoingPublisher, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val outgoingNotifier = new OutgoingPublisher(operationName, snsWriter)

      testWith(outgoingNotifier)
    }

  def withOperationReporter[R]()(testWith: TestWith[DiagnosticReporter, R]): R =
    withMetricsSender { metricsSender =>
      val reporter = new DiagnosticReporter(metricsSender)

      testWith(reporter)
    }
}
