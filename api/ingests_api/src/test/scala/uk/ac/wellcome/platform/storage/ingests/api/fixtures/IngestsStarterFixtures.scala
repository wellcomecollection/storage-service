package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.monitor.{IngestTracker, MemoryIngestTracker}
import uk.ac.wellcome.platform.storage.ingests.api.IngestStarter

trait IngestsStarterFixtures extends RandomThings {
  def createMessageSender =
    new MemoryMessageSender(
      destination = randomAlphanumeric(),
      subject = randomAlphanumeric()
    )

  def createIngestTracker = new MemoryIngestTracker()

  def createIngestStarter(
                           sender: MessageSender[String] = createMessageSender,
                           tracker: IngestTracker = createIngestTracker
                         ): IngestStarter[String] = new IngestStarter[String](
    ingestTracker = tracker,
    unpackerMessageSender = sender
  )
}
