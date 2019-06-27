package uk.ac.wellcome.platform.archive.common.ingests.tracker.memory

import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.BetterIngestTracker
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

class MemoryIngestTracker(initialIngests: Seq[Ingest])
  extends BetterIngestTracker[MemoryVersionedStore[IngestID, Int, Ingest]] {

  override val underlying: MemoryVersionedStore[IngestID, Int, Ingest] = {
    val initialEntries =
      initialIngests
        .map { ingest => (Version(ingest.id, 0), ingest) }
        .toMap

    new MemoryVersionedStore[IngestID, Int, Ingest](
      store = new MemoryStore[Version[IngestID, Int], Ingest](initialEntries)
        with MemoryMaxima[IngestID, Ingest]
    )
  }
}
