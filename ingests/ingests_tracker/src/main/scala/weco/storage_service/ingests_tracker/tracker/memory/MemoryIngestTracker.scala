package weco.storage_service.ingests_tracker.tracker.memory

import weco.storage_service.ingests.models.{Ingest, IngestID}
import weco.storage_service.ingests_tracker.tracker.IngestTracker
import weco.storage.store.memory.MemoryVersionedStore

class MemoryIngestTracker(
  val underlying: MemoryVersionedStore[IngestID, Ingest]
) extends IngestTracker
