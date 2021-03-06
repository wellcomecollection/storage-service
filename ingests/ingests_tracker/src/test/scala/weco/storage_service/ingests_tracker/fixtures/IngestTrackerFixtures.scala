package weco.storage_service.ingests_tracker.fixtures

import org.scalatest.EitherValues
import weco.fixtures.TestWith
import weco.storage_service.ingests.models.{Ingest, IngestID}
import weco.storage_service.ingests_tracker.tracker.memory.MemoryIngestTracker
import weco.storage.Version
import weco.storage.maxima.memory.MemoryMaxima
import weco.storage.store.memory.{MemoryStore, MemoryVersionedStore}
import weco.fixtures.TimeAssertions

trait IngestTrackerFixtures extends EitherValues with TimeAssertions {
  def assertIngestCreated(
    ingest: Ingest
  )(implicit ingestTracker: MemoryIngestTracker): Ingest = {
    val storedIngest =
      ingestTracker.underlying.getLatest(ingest.id).value.identifiedT

    storedIngest.sourceLocation shouldBe ingest.sourceLocation
    storedIngest.sourceLocation shouldBe ingest.sourceLocation

    assertRecent(storedIngest.createdDate, recentSeconds = 45)
    storedIngest.lastModifiedDate.map { lastModifiedDate =>
      assertRecent(lastModifiedDate, recentSeconds = 45)
    }
    storedIngest
  }

  def assertIngestRecordedRecentEvents(
    id: IngestID,
    expectedEventDescriptions: Seq[String]
  )(implicit ingestTracker: MemoryIngestTracker): Unit = {
    val storedIngest =
      ingestTracker.underlying.getLatest(id).value.identifiedT

    storedIngest.events.map { _.description } should contain theSameElementsAs expectedEventDescriptions
    storedIngest.events.foreach { event =>
      assertRecent(event.createdDate, recentSeconds = 45)
    }
  }

  protected def createMemoryStore: MemoryStore[Version[IngestID, Int], Ingest]
    with MemoryMaxima[IngestID, Ingest] =
    new MemoryStore[Version[IngestID, Int], Ingest](initialEntries = Map.empty)
    with MemoryMaxima[IngestID, Ingest]

  protected def createMemoryVersionedStore
    : MemoryVersionedStore[IngestID, Ingest] =
    new MemoryVersionedStore[IngestID, Ingest](createMemoryStore)

  def withMemoryIngestTracker[R](
    initialIngests: Seq[Ingest] = Seq.empty
  )(testWith: TestWith[MemoryIngestTracker, R])(
    implicit store: MemoryVersionedStore[IngestID, Ingest] =
      createMemoryVersionedStore
  ): R =
    testWith(
      createMemoryIngestTrackerWith(initialIngests)
    )

  def createMemoryIngestTrackerWith(initialIngests: Seq[Ingest])(
    implicit store: MemoryVersionedStore[IngestID, Ingest] =
      createMemoryVersionedStore
  ): MemoryIngestTracker = {
    initialIngests
      .map { ingest =>
        store.init(ingest.id)(ingest) shouldBe a[Right[_, _]]
      }

    new MemoryIngestTracker(store)
  }
}
