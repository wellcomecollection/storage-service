package uk.ac.wellcome.platform.archive.common.ingests.monitor

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models._

import scala.util.{Failure, Success}

class MemoryIngestTrackerTest extends FunSpec with Matchers with IngestGenerators {
  describe("create") {
    it("creates an ingest") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest) shouldBe Success(ingest)

      tracker.ingests shouldBe Map(ingest.id -> ingest)
    }

    it("creates only one ingest for a given id") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest) shouldBe Success(ingest)
      tracker.initialise(ingest) shouldBe a[Failure[_]]

      tracker.ingests shouldBe Map(ingest.id -> ingest)
    }
  }

  describe("read") {
    it("retrieves ingest by id") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest)

      tracker.get(ingest.id) shouldBe Success(Some(ingest))
    }

    it("returns None when no ingest matches id") {
      val tracker = new MemoryIngestTracker()
      tracker.get(createIngestID) shouldBe Success(None)
    }
  }

  describe("update") {
    it("updates the bag ID on an ingest that didn't have one") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest)

      tracker.ingests(ingest.id).bag shouldBe None

          val bagId = createBagId

          val ingestUpdate = IngestStatusUpdate(
            id = ingest.id,
            status = Ingest.Processing,
            affectedBag = Some(bagId)
          )

      val result = tracker.update(ingestUpdate)
      result shouldBe a[Success[_]]
      val storedIngest = result.get

      tracker.ingests shouldBe Map(ingest.id -> storedIngest)
      storedIngest.bag shouldBe Some(bagId)
    }

    it("adds a single event to an Ingest with no events") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest)

      val ingestUpdate = IngestEventUpdate(
        id = ingest.id,
        events = List(createIngestEvent)
      )

      tracker.update(ingestUpdate) shouldBe a[Success[_]]

      val storedIngest: Ingest = tracker.ingests(ingest.id)
      storedIngest.events shouldBe ingestUpdate.events
    }

    it("adds a status update to an Ingest with no events") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest)

      val someBagId = Some(createBagId)
      val ingestUpdate = IngestStatusUpdate(
        id = ingest.id,
        status = Ingest.Completed,
        affectedBag = someBagId,
        events = List(createIngestEvent)
      )

      tracker.update(ingestUpdate) shouldBe a[Success[_]]

      val storedIngest: Ingest = tracker.ingests(ingest.id)
      storedIngest.status shouldBe Ingest.Completed
      storedIngest.bag shouldBe someBagId

      storedIngest.events shouldBe ingestUpdate.events
    }

    it("adds a callback status update to an Ingest with no events") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest)

      val ingestUpdate = IngestCallbackStatusUpdate(
        id = ingest.id,
        callbackStatus = Callback.Succeeded,
        events = List(createIngestEvent)
      )

      tracker.update(ingestUpdate) shouldBe a[Success[_]]

      val storedIngest: Ingest = tracker.ingests(ingest.id)

      storedIngest.callback shouldBe defined
      storedIngest.callback.get.status shouldBe Callback.Succeeded
      storedIngest.events shouldBe ingestUpdate.events
    }

    it("adds an update with multiple events") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest)

      val ingestUpdate = IngestEventUpdate(
        ingest.id,
        List(createIngestEvent, createIngestEvent)
      )

      tracker.update(ingestUpdate) shouldBe a[Success[_]]

      val storedIngest: Ingest = tracker.ingests(ingest.id)
      storedIngest.events shouldBe ingestUpdate.events
    }

    it("adds multiple events to an Ingest") {
      val tracker = new MemoryIngestTracker()

      val ingest = createIngest
      tracker.initialise(ingest)

          val updates = List(
            createIngestEventUpdateWith(ingest.id),
            createIngestEventUpdateWith(ingest.id)
          )

          updates.foreach { tracker.update(_) }

      val storedIngest: Ingest = tracker.ingests(ingest.id)
      storedIngest.events shouldBe updates(0).events ++ updates(1).events
    }
  }

  describe("find ingest by BagId") {
    it(
      "finds ingests with a matching bag ID") {
      val tracker = new MemoryIngestTracker()

      val bagId1 = createBagId
      val bagId2 = createBagId

      val ingest0 = createIngest
      val ingest1A = createIngestWith(maybeBag = Some(bagId1))
      val ingest1B = createIngestWith(maybeBag = Some(bagId1))
      val ingest1C = createIngestWith(maybeBag = Some(bagId1))
      val ingest2A = createIngestWith(maybeBag = Some(bagId2))
      val ingest2B = createIngestWith(maybeBag = Some(bagId2))

      Seq(ingest0, ingest1A, ingest1B, ingest1C, ingest2A, ingest2B).map { ingest =>
        tracker.initialise(ingest)
      }

      val result1 = tracker.findByBagId(bagId1)
      result1 shouldBe a[Success[_]]
      result1.get.map { _.id } should contain theSameElementsAs Seq(ingest1A, ingest1B, ingest1C).map { _.id }

      val result2 = tracker.findByBagId(bagId2)
      result2 shouldBe a[Success[_]]
      result2.get.map { _.id } should contain theSameElementsAs Seq(ingest2A, ingest2B).map { _.id }
    }
  }
}
