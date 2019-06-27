package uk.ac.wellcome.platform.archive.common.ingests.tracker

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.{Identified, StoreReadError, StoreWriteError, Version}
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

trait BetterIngestTrackerTestCases[StoreImpl <: VersionedStore[IngestID, Int, Ingest]] extends FunSpec with Matchers with EitherValues with IngestGenerators with TableDrivenPropertyChecks {
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

  describe("update()") {
    describe("IngestEventUpdate") {
      it("adds an event to an ingest") {
        val ingest = createIngestWith(events = List.empty)

        val event = createIngestEvent
        val update = createIngestEventUpdateWith(
          id = ingest.id,
          events = List(event)
        )

        withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
          val result = tracker.update(update)
          result.right.value.identifiedT.events shouldBe Seq(event)

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          storedIngest.events shouldBe Seq(event)
        }
      }

      it("adds multiple events to an ingest") {
        val ingest = createIngestWith(events = List.empty)

        val events = List(createIngestEvent, createIngestEvent, createIngestEvent)
        val update = createIngestEventUpdateWith(
          id = ingest.id,
          events = events
        )

        withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
          val result = tracker.update(update)
          result.right.value.identifiedT.events shouldBe events

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          storedIngest.events shouldBe events
        }
      }

      it("preserves the existing events on an ingest") {
        val existingEvents = List(createIngestEvent, createIngestEvent)
        val ingest = createIngestWith(events = existingEvents)

        val newEvents = List(createIngestEvent, createIngestEvent)
        val update = createIngestEventUpdateWith(
          id = ingest.id,
          events = newEvents
        )

        withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
          val result = tracker.update(update)
          result.right.value.identifiedT.events shouldBe existingEvents ++ newEvents

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          storedIngest.events shouldBe existingEvents ++ newEvents
        }
      }

      it("errors if there is no existing ingest with this ID") {
        val update = createIngestEventUpdate

        withIngestTrackerFixtures() { tracker =>
          tracker.update(update).left.value shouldBe a[UpdateNonExistentIngestError]
        }
      }
    }

    describe("IngestStatusUpdate") {
      it("adds an event to an ingest") {
        val ingest = createIngestWith(events = List.empty)

        val event = createIngestEvent
        val update = createIngestStatusUpdateWith(
          id = ingest.id,
          events = List(event)
        )

        withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
          val result = tracker.update(update)
          result.right.value.identifiedT.events shouldBe Seq(event)

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          storedIngest.events shouldBe Seq(event)
        }
      }

      it("adds multiple events to an ingest") {
        val ingest = createIngestWith(events = List.empty)

        val events = List(createIngestEvent, createIngestEvent, createIngestEvent)
        val update = createIngestStatusUpdateWith(
          id = ingest.id,
          events = events
        )

        withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
          val result = tracker.update(update)
          result.right.value.identifiedT.events shouldBe events

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          storedIngest.events shouldBe events
        }
      }

      it("preserves the existing events on an ingest") {
        val existingEvents = List(createIngestEvent, createIngestEvent)
        val ingest = createIngestWith(events = existingEvents)

        val newEvents = List(createIngestEvent, createIngestEvent)
        val update = createIngestStatusUpdateWith(
          id = ingest.id,
          events = newEvents
        )

        withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
          val result = tracker.update(update)
          result.right.value.identifiedT.events shouldBe existingEvents ++ newEvents

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          storedIngest.events shouldBe existingEvents ++ newEvents
        }
      }

      it("updates the bag ID on an ingest") {
        true shouldBe false
      }

      it("updates if the bag ID is already set and matches the update") {
        true shouldBe false
      }

      it("errors if there is no existing ingest with this ID") {
        true shouldBe false
      }

      it("errors if the existing bag ID is set and is different") {
        true shouldBe false
      }

      val allowedStatusUpdates = Table(
        ("initial", "update"),
        (Ingest.Accepted, Ingest.Accepted),
        (Ingest.Accepted, Ingest.Processing),
        (Ingest.Accepted, Ingest.Completed),
        (Ingest.Accepted, Ingest.Failed),
        (Ingest.Processing, Ingest.Completed),
        (Ingest.Processing, Ingest.Failed),
      )

      it("updates the status of an ingest") {
        forAll(allowedStatusUpdates) {
          case (initialStatus: Ingest.Status, updatedStatus: Ingest.Status) =>
            val ingest = createIngestWith(status = initialStatus)

            val update = createIngestStatusUpdateWith(
              id = ingest.id,
              status = updatedStatus
            )

            withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
              val result = tracker.update(update)
              result.right.value.identifiedT.status shouldBe updatedStatus
            }
        }
      }

      val disallowedStatusUpdates = Table(
        ("initial", "update"),
        (Ingest.Failed, Ingest.Completed),
        (Ingest.Failed, Ingest.Processing),
        (Ingest.Failed, Ingest.Accepted),
        (Ingest.Completed, Ingest.Failed),
        (Ingest.Completed, Ingest.Processing),
        (Ingest.Completed, Ingest.Accepted),
        (Ingest.Processing, Ingest.Accepted),
      )

      it("does not allow the status to go backwards") {
        forAll(disallowedStatusUpdates) {
          case (initialStatus: Ingest.Status, updatedStatus: Ingest.Status) =>
            val ingest = createIngestWith(status = initialStatus)

            val update = createIngestStatusUpdateWith(
              id = ingest.id,
              status = updatedStatus
            )

            withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
              val result = tracker.update(update)
              result.left.value shouldBe a[IngestStatusGoingBackwards]
            }
        }
      }
    }

    describe("CallbackStatusUpdate") {
      it("adds a callback status to an ingest") {
        true shouldBe false
      }

      it("passes if the status is already set and matches the update") {
        true shouldBe false
      }

      it("errors if the status is already set and is different") {
        true shouldBe false
      }

      it("errors if the ingest does not have a callback") {
        true shouldBe false
      }

      it("errors if there is no existing ingest with this ID") {
        true shouldBe false
      }
    }

    it("wraps an error from the underlying update() method") {
      true shouldBe false
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
