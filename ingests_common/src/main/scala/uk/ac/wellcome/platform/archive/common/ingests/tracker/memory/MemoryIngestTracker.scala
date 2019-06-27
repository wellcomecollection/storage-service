package uk.ac.wellcome.platform.archive.common.ingests.tracker.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestTracker, IngestTrackerError}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

class MemoryIngestTracker(val underlying: MemoryVersionedStore[IngestID, Int, Ingest])
  extends IngestTracker {

  override def listByBagId(bagId: BagId): Either[IngestTrackerError, Seq[Ingest]] =
    Right(
      underlying.store
        .asInstanceOf[MemoryStore[Version[IngestID, Int], Ingest]]
        .entries.values
        .filter { _.bag.contains(bagId) }
        .toSeq
    )
}
