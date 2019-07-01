package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTracker
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests.api.IngestStarter

trait IngestStarterFixture extends IngestTrackerFixtures {
  def withIngestStarter[R](
    ingestTracker: IngestTracker,
    messageSender: MemoryMessageSender
  )(
    testWith: TestWith[IngestStarter[String], R]
  ): R = {
    val ingestStarter = new IngestStarter(
      ingestTracker = ingestTracker,
      unpackerMessageSender = messageSender
    )

    testWith(ingestStarter)
  }
}
