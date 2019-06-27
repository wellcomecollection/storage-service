package uk.ac.wellcome.platform.archive.common.ingests.tracker.memory

import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.BetterIngestTracker
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

class MemoryIngestTracker(val underlying: MemoryVersionedStore[IngestID, Int, Ingest])
  extends BetterIngestTracker
