package uk.ac.wellcome.platform.archive.common.ingests.tracker.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.{
  IngestTracker,
  IngestTrackerTestCases
}
import uk.ac.wellcome.storage.{
  StoreReadError,
  StoreWriteError,
  UpdateWriteError,
  Version
}
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

class MemoryIngestTrackerTest
    extends IngestTrackerTestCases[MemoryVersionedStore[IngestID, Int, Ingest]] {
  private def createMemoryStore =
    new MemoryStore[Version[IngestID, Int], Ingest](initialEntries = Map.empty)
    with MemoryMaxima[IngestID, Ingest]

  override def withContext[R](
    testWith: TestWith[MemoryVersionedStore[IngestID, Int, Ingest], R]): R =
    testWith(new MemoryVersionedStore[IngestID, Int, Ingest](createMemoryStore))

  override def withIngestTracker[R](initialIngests: Seq[Ingest])(
    testWith: TestWith[IngestTracker, R])(
    implicit store: MemoryVersionedStore[IngestID, Int, Ingest]): R = {

    initialIngests
      .map { ingest =>
        store.init(ingest.id)(ingest)
      }

    testWith(new MemoryIngestTracker(store))
  }

  override def withBrokenInitStoreContext[R](
    testWith: TestWith[MemoryVersionedStore[IngestID, Int, Ingest], R]): R =
    testWith(
      new MemoryVersionedStore[IngestID, Int, Ingest](createMemoryStore) {
        override def init(id: IngestID)(t: Ingest) =
          Left(StoreWriteError(new Throwable("BOOM!")))
      }
    )

  override def withBrokenGetStoreContext[R](
    testWith: TestWith[MemoryVersionedStore[IngestID, Int, Ingest], R]): R =
    testWith(
      new MemoryVersionedStore[IngestID, Int, Ingest](createMemoryStore) {
        override def getLatest(id: IngestID) =
          Left(StoreReadError(new Throwable("BOOM!")))
      }
    )

  override def withBrokenUpdateStoreContext[R](
    testWith: TestWith[MemoryVersionedStore[IngestID, Int, Ingest], R]): R =
    testWith(
      new MemoryVersionedStore[IngestID, Int, Ingest](createMemoryStore) {
        override def update(id: IngestID)(f: Ingest => Ingest) =
          Left(UpdateWriteError(new Throwable("BOOM!")))
      }
    )
}
