package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher

trait OperationFixtures extends RandomThings {
  type Destination = String

  def createStepName: String = randomAlphanumeric()

  def createMessageSender: MemoryMessageSender = new MemoryMessageSender(
    destination = randomAlphanumeric(),
    subject = randomAlphanumeric()
  )

  def createIngestUpdater(stepName: String = createStepName, messageSender: MessageSender[Destination] = createMessageSender): IngestUpdater[Destination] =
    new IngestUpdater[String](
      stepName = stepName,
      messageSender = messageSender
    )

  def createOperationName: String = randomAlphanumeric()

  def createOutgoingPublisher(operationName: String = createOperationName, messageSender: MessageSender[Destination] = createMessageSender): OutgoingPublisher[Destination] =
    new OutgoingPublisher[Destination](
      operationName = operationName,
      messageSender = messageSender
    )
}
