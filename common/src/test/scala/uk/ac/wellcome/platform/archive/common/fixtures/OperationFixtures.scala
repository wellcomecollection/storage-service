package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher

trait OperationFixtures extends StorageRandomThings {
  def createIngestUpdaterWith(messageSender: MemoryMessageSender,
                              stepName: String = randomAlphanumericWithLength()) =
    new IngestUpdater[String](
      stepName = stepName,
      messageSender = messageSender
    )

  def createOutgoingPublisherWith(messageSender: MemoryMessageSender) =
    new OutgoingPublisher(messageSender)
}
