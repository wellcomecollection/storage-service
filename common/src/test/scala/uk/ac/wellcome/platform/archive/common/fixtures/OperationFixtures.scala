package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher

import scala.concurrent.ExecutionContext.Implicits.global

trait OperationFixtures extends SNS with MetricsSenderFixture {
  def withIngestUpdater[R](stepName: String, topic: Topic)(
    testWith: TestWith[IngestUpdater, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val ingestNotifier = new IngestUpdater(stepName, snsWriter)

      testWith(ingestNotifier)
    }

  def withOutgoingPublisher[R](operationName: String, topic: Topic)(
    testWith: TestWith[OutgoingPublisher, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val outgoingNotifier = new OutgoingPublisher(operationName, snsWriter)

      testWith(outgoingNotifier)
    }
}
