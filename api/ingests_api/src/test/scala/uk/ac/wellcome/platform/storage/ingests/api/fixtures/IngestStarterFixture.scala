package uk.ac.wellcome.platform.storage.ingests.api.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.storage.ingests.api.IngestStarter
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

trait IngestStarterFixture extends IngestTrackerFixture {
  def withIngestStarter[R](
    table: Table,
    messageSender: MemoryMessageSender
  )(
    testWith: TestWith[IngestStarter[String], R]
  ): R =
    withIngestTracker(table) { ingestTracker =>
      val ingestStarter = new IngestStarter(
        ingestTracker = ingestTracker,
        unpackerMessageSender = messageSender
      )

      testWith(ingestStarter)
    }
}
