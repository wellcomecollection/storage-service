package uk.ac.wellcome.platform.storage.ingests_tracker.tracker.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.{
  IngestTracker,
  IngestTrackerTestCases
}
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore
import uk.ac.wellcome.storage.{
  StoreReadError,
  StoreWriteError,
  UpdateWriteError
}

class MemoryIngestTrackerTest
    extends IngestTrackerTestCases[MemoryVersionedStore[IngestID, Ingest]]
    with IngestTrackerFixtures {

  override def withContext[R](
    testWith: TestWith[MemoryVersionedStore[IngestID, Ingest], R]
  ): R =
    testWith(createMemoryVersionedStore)

  override def withIngestTracker[R](initialIngests: Seq[Ingest])(
    testWith: TestWith[IngestTracker, R]
  )(implicit store: MemoryVersionedStore[IngestID, Ingest]): R =
    withMemoryIngestTracker(initialIngests) { tracker =>
      testWith(tracker)
    }

  override def withBrokenUnderlyingInitTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit context: MemoryVersionedStore[IngestID, Ingest]): R =
    testWith(
      new MemoryIngestTracker(
        new MemoryVersionedStore[IngestID, Ingest](createMemoryStore) {
          override def init(id: IngestID)(t: Ingest) =
            Left(StoreWriteError(new Throwable("BOOM!")))
        }
      )
    )

  override def withBrokenUnderlyingGetTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit context: MemoryVersionedStore[IngestID, Ingest]): R =
    testWith(
      new MemoryIngestTracker(
        new MemoryVersionedStore[IngestID, Ingest](createMemoryStore) {
          override def getLatest(id: IngestID) =
            Left(StoreReadError(new Throwable("BOOM!")))
        }
      )
    )

  override def withBrokenUnderlyingUpdateTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit context: MemoryVersionedStore[IngestID, Ingest]): R =
    testWith(
      new MemoryIngestTracker(
        new MemoryVersionedStore[IngestID, Ingest](createMemoryStore) {
          override def update(id: IngestID)(f: UpdateFunction): UpdateEither =
            Left(UpdateWriteError(StoreWriteError(new Throwable("BOOM!"))))
        }
      )
    )
}
