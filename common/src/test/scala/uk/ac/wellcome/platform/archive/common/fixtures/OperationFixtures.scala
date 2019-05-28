package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher

trait OperationFixtures {
  def withIngestUpdater[R](stepName: String, messageSender: MemoryMessageSender)(
    testWith: TestWith[IngestUpdater[String], R]): R =
    testWith(new IngestUpdater[String](stepName, messageSender))

  def withOutgoingPublisher[R](messageSender: MemoryMessageSender)(
    testWith: TestWith[OutgoingPublisher[String], R]): R =
      testWith(new OutgoingPublisher(messageSender))
}
