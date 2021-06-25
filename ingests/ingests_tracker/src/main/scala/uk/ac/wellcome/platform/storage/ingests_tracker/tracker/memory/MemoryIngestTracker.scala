package uk.ac.wellcome.platform.storage.ingests_tracker.tracker.memory

import weco.storage_service.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.IngestTracker
import weco.storage.store.memory.MemoryVersionedStore

class MemoryIngestTracker(
  val underlying: MemoryVersionedStore[IngestID, Ingest]
) extends IngestTracker
