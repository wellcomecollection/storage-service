package uk.ac.wellcome.platform.archive.common.ingests.tracker

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.storage.{Identified, Version}
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

trait BetterIngestTrackerTestCases[StoreImpl <: VersionedStore[IngestID, Int, Ingest], Context] extends FunSpec with Matchers with EitherValues with IngestGenerators {
  def withIngestTrackerContext[R](testWith: TestWith[Context, R]): R

  def withIngestTracker[R](initialIngests: Seq[Ingest] = Seq.empty)(testWith: TestWith[BetterIngestTracker[StoreImpl], R])(implicit context: Context): R

  private def withIngestTrackerFixtures[R](initialIngests: Seq[Ingest] = Seq.empty)(testWith: TestWith[BetterIngestTracker[StoreImpl], R]): R =
    withIngestTrackerContext { implicit context =>
      withIngestTracker(initialIngests) { tracker =>
        testWith(tracker)
      }
    }

  describe("initialising the ingest") {
    it("creates an ingest") {
      withIngestTrackerFixtures() { tracker =>
        val ingest = createIngest
        tracker.init(ingest).right.value shouldBe Identified(Version(ingest.id, 0), ingest)
      }
    }
  }
}

class MemoryIngestTrackerTest extends BetterIngestTrackerTestCases[MemoryVersionedStore[IngestID, Int, Ingest], MemoryIngestTracker] {
  override def withIngestTrackerContext[R](testWith: TestWith[MemoryIngestTracker, R]): R =
    testWith(new MemoryIngestTracker(initialIngests = Seq.empty))

  override def withIngestTracker[R](initialIngests: Seq[Ingest])(
    testWith: TestWith[BetterIngestTracker[MemoryVersionedStore[IngestID, Int, Ingest]], R])(
    implicit context: MemoryIngestTracker): R = {
    initialIngests
      .map { ingest => context.underlying.init(ingest.id)(ingest) }

    testWith(context)
  }
}
