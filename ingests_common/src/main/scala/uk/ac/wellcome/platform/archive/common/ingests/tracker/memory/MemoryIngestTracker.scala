package uk.ac.wellcome.platform.archive.common.ingests.tracker.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestStoreError,
  IngestTracker
}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

class MemoryIngestTracker(
  val underlying: MemoryVersionedStore[IngestID, Ingest]
) extends IngestTracker {

  override def listByBagId(
    bagId: BagId
  ): Either[IngestStoreError, Seq[Ingest]] =
    Right(
      underlying.store
        .asInstanceOf[MemoryStore[Version[IngestID, Int], Ingest]]
        .entries
        .values
        .filter { storedIngest =>
          storedIngest.externalIdentifier == bagId.externalIdentifier &&
          storedIngest.space == bagId.space
        }
        .toSeq
        .sortBy { _.createdDate }
        .reverse
    )
}

object MemoryIngestTracker {
  def apply(): MemoryIngestTracker =
    new MemoryIngestTracker(
      underlying = MemoryVersionedStore[IngestID, Ingest](
        initialEntries = Map.empty
      )
    )
}
