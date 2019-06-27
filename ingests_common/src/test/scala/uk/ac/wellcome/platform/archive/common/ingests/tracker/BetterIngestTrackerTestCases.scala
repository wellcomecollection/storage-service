package uk.ac.wellcome.platform.archive.common.ingests.tracker

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.{Identified, StoreReadError, StoreWriteError, Version}
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

trait BetterIngestTrackerTestCases[StoreImpl <: VersionedStore[IngestID, Int, Ingest]] extends FunSpec with Matchers with EitherValues with IngestGenerators {
  def withStoreImpl[R](testWith: TestWith[StoreImpl, R]): R

  def withIngestTracker[R](initialIngests: Seq[Ingest] = Seq.empty)(testWith: TestWith[BetterIngestTracker, R])(implicit store: StoreImpl): R

  private def withIngestTrackerFixtures[R](initialIngests: Seq[Ingest] = Seq.empty)(testWith: TestWith[BetterIngestTracker, R]): R =
    withStoreImpl { implicit store =>
      withIngestTracker(initialIngests) { tracker =>
        testWith(tracker)
      }
    }

  def withBrokenInitStoreImpl[R](testWith: TestWith[StoreImpl, R]): R
  def withBrokenGetStoreImpl[R](testWith: TestWith[StoreImpl, R]): R

  describe("init()") {
    it("creates an ingest") {
      withIngestTrackerFixtures() { tracker =>
        val ingest = createIngest
        tracker.init(ingest).right.value shouldBe Identified(Version(ingest.id, 0), ingest)
      }
    }

    it("only allows calling init() once") {
      withIngestTrackerFixtures() { tracker =>
        val ingest = createIngest
        tracker.init(ingest)

        tracker.init(ingest).left.value shouldBe a[IngestAlreadyExistsError]
      }
    }

    it("blocks calling init() on a pre-existing ingest") {
      val ingest = createIngest

      withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
        tracker.init(ingest).left.value shouldBe a[IngestAlreadyExistsError]
      }
    }

    it("wraps an init() error from the underlying store") {
      withBrokenInitStoreImpl { implicit store =>
        withIngestTracker() { tracker =>
          tracker.init(createIngest).left.value shouldBe a[IngestTrackerStoreError]
        }
      }
    }
  }

  describe("get()") {
    it("finds an ingest") {
      val ingest = createIngest

      withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
        tracker.get(ingest.id).right.value shouldBe Identified(Version(ingest.id, 0), ingest)
      }
    }

    it("returns an IngestNotFound if the ingest doesn't exist") {
      withIngestTrackerFixtures() { tracker =>
        tracker.get(createIngestID).left.value shouldBe a[IngestDoesNotExistError]
      }
    }

    it("wraps a get() error from the underlying store") {
      withBrokenGetStoreImpl { implicit store =>
        withIngestTracker() { tracker =>
          tracker.get(createIngestID).left.value shouldBe a[IngestTrackerStoreError]
        }
      }
    }
  }
}

class MemoryIngestTrackerTest extends BetterIngestTrackerTestCases[MemoryVersionedStore[IngestID, Int, Ingest]] {
  private def createMemoryStore =
    new MemoryStore[Version[IngestID, Int], Ingest](initialEntries = Map.empty) with MemoryMaxima[IngestID, Ingest]

  override def withStoreImpl[R](testWith: TestWith[MemoryVersionedStore[IngestID, Int, Ingest], R]): R =
    testWith(new MemoryVersionedStore[IngestID, Int, Ingest](createMemoryStore))

  override def withIngestTracker[R](initialIngests: Seq[Ingest])(
    testWith: TestWith[BetterIngestTracker, R])(
    implicit store: MemoryVersionedStore[IngestID, Int, Ingest]): R = {

    initialIngests
      .map { ingest => store.init(ingest.id)(ingest) }

    testWith(new MemoryIngestTracker(store))
  }

  override def withBrokenInitStoreImpl[R](testWith: TestWith[MemoryVersionedStore[IngestID, Int, Ingest], R]): R =
    testWith(
      new MemoryVersionedStore[IngestID, Int, Ingest](createMemoryStore) {
        override def init(id: IngestID)(t: Ingest) = Left(StoreWriteError(new Throwable("BOOM!")))
      }
    )

  override def withBrokenGetStoreImpl[R](testWith: TestWith[MemoryVersionedStore[IngestID, Int, Ingest], R]): R =
    testWith(
      new MemoryVersionedStore[IngestID, Int, Ingest](createMemoryStore) {
        override def getLatest(id: IngestID) = Left(StoreReadError(new Throwable("BOOM!")))
      }
    )
}
