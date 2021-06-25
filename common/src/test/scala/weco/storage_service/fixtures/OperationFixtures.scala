package weco.storage_service.fixtures

import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage_service.ingests.services.IngestUpdater
import weco.storage_service.operation.services.OutgoingPublisher

trait OperationFixtures extends StorageRandomGenerators {
  def createIngestUpdaterWith(
    messageSender: MemoryMessageSender,
    stepName: String = createStepName
  ) =
    new IngestUpdater[String](
      stepName = stepName,
      messageSender = messageSender
    )

  def createOutgoingPublisherWith(messageSender: MemoryMessageSender) =
    new OutgoingPublisher(messageSender)
}
