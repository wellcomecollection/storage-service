package uk.ac.wellcome.platform.storage.ingests_tracker.tracker.memory

import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.IngestTracker
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

class MemoryIngestTracker(
  val underlying: MemoryVersionedStore[IngestID, Ingest]
) extends IngestTracker

object MemoryIngestTracker {
  def apply(): MemoryIngestTracker =
    new MemoryIngestTracker(
      underlying = MemoryVersionedStore[IngestID, Ingest](
        initialEntries = Map.empty
      )
    )
}
