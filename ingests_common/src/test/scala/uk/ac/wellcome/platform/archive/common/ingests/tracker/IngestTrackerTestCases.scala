package uk.ac.wellcome.platform.archive.common.ingests.tracker

import java.net.URI

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Callback,
  Ingest
}
import uk.ac.wellcome.storage._

trait IngestTrackerTestCases[Context]
    extends FunSpec
    with Matchers
    with EitherValues
    with IngestGenerators
    with TableDrivenPropertyChecks {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withIngestTracker[R](initialIngests: Seq[Ingest] = Seq.empty)(
    testWith: TestWith[IngestTracker, R])(implicit context: Context): R

  private def withIngestTrackerFixtures[R](initialIngests: Seq[Ingest] =
                                             Seq.empty)(
    testWith: TestWith[IngestTracker, R]): R =
    withContext { implicit context =>
      withIngestTracker(initialIngests) { tracker =>
        testWith(tracker)
      }
    }

  def withBrokenInitStoreContext[R](testWith: TestWith[Context, R]): R
  def withBrokenGetStoreContext[R](testWith: TestWith[Context, R]): R
  def withBrokenUpdateStoreContext[R](testWith: TestWith[Context, R]): R

  describe("init()") {
    it("creates an ingest") {
      withIngestTrackerFixtures() { tracker =>
        val ingest = createIngest
        tracker.init(ingest).right.value shouldBe Identified(
          Version(ingest.id, 0),
          ingest)
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
      withBrokenInitStoreContext { implicit context =>
        withIngestTracker() { tracker =>
          tracker
            .init(createIngest)
            .left
            .value shouldBe a[IngestTrackerStoreError]
        }
      }
    }
  }

  describe("get()") {
    it("finds an ingest") {
      val ingest = createIngest

      withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
        tracker.get(ingest.id).right.value shouldBe Identified(
          Version(ingest.id, 0),
          ingest)
      }
    }

    it("returns an IngestNotFound if the ingest doesn't exist") {
      withIngestTrackerFixtures() { tracker =>
        tracker
          .get(createIngestID)
          .left
          .value shouldBe a[IngestDoesNotExistError]
      }
    }

    it("wraps a get() error from the underlying store") {
      withBrokenGetStoreContext { implicit context =>
        withIngestTracker() { tracker =>
          tracker
            .get(createIngestID)
            .left
            .value shouldBe a[IngestTrackerStoreError]
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

        val events =
          List(createIngestEvent, createIngestEvent, createIngestEvent)
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
          tracker
            .update(update)
            .left
            .value shouldBe a[UpdateNonExistentIngestError]
        }
      }
    }

    describe("IngestStatusUpdate") {
      describe("updating the events") {
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

          val events =
            List(createIngestEvent, createIngestEvent, createIngestEvent)
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
      }

      describe("updating the bag ID") {
        it("adds the bag ID if there isn't one already") {
          val ingest = createIngestWith(maybeBag = None)
          val bagId = createBagId

          val update = createIngestStatusUpdateWith(
            id = ingest.id,
            maybeBag = Some(bagId)
          )

          withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
            val result = tracker.update(update)
            result.right.value.identifiedT.bag shouldBe Some(bagId)
          }
        }

        it("does not remove an existing bag ID") {
          val bagId = createBagId
          val ingest = createIngestWith(maybeBag = Some(bagId))

          val update = createIngestStatusUpdateWith(
            id = ingest.id,
            maybeBag = None
          )

          withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
            val result = tracker.update(update)
            result.right.value.identifiedT.bag shouldBe Some(bagId)
          }
        }

        it("updates if the bag ID is already set and matches the update") {
          val bagId = createBagId
          val ingest = createIngestWith(maybeBag = Some(bagId))

          val update = createIngestStatusUpdateWith(
            id = ingest.id,
            maybeBag = Some(bagId)
          )

          withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
            val result = tracker.update(update)
            result.right.value.identifiedT.bag shouldBe Some(bagId)
          }
        }

        it("errors if the existing bag ID is set and is different") {
          val ingest = createIngestWith(maybeBag = Some(createBagId))

          val update = createIngestStatusUpdateWith(
            id = ingest.id,
            maybeBag = Some(createBagId)
          )

          withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
            val result = tracker.update(update)
            result.left.value shouldBe MismatchedBagIdError(
              stored = ingest.bag.get,
              update = update.affectedBag.get
            )
          }
        }
      }

      describe("updating the status") {
        val allowedStatusUpdates = Table(
          ("initial", "update"),
          (Ingest.Accepted, Ingest.Accepted),
          (Ingest.Accepted, Ingest.Processing),
          (Ingest.Accepted, Ingest.Completed),
          (Ingest.Accepted, Ingest.Failed),
          (Ingest.Processing, Ingest.Completed),
          (Ingest.Processing, Ingest.Failed),
          (Ingest.Completed, Ingest.Completed),
          (Ingest.Failed, Ingest.Failed),
        )

        it("updates the status of an ingest") {
          forAll(allowedStatusUpdates) {
            case (initialStatus: Ingest.Status, updatedStatus: Ingest.Status) =>
              val ingest = createIngestWith(status = initialStatus)

              val update = createIngestStatusUpdateWith(
                id = ingest.id,
                status = updatedStatus
              )

              withIngestTrackerFixtures(initialIngests = Seq(ingest)) {
                tracker =>
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

              withIngestTrackerFixtures(initialIngests = Seq(ingest)) {
                tracker =>
                  val result = tracker.update(update)
                  result.left.value shouldBe a[IngestStatusGoingBackwards]
              }
          }
        }
      }

      it("errors if there is no existing ingest with this ID") {
        val update = createIngestStatusUpdate

        withIngestTrackerFixtures() { tracker =>
          val result = tracker.update(update)
          result.left.value shouldBe a[UpdateNonExistentIngestError]
        }
      }
    }

    describe("CallbackStatusUpdate") {
      describe("updating the events") {
        it("adds an event to an ingest") {
          val ingest = createIngestWith(events = List.empty)

          val event = createIngestEvent
          val update = createIngestCallbackStatusUpdateWith(
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

          val events =
            List(createIngestEvent, createIngestEvent, createIngestEvent)
          val update = createIngestCallbackStatusUpdateWith(
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
          val update = createIngestCallbackStatusUpdateWith(
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
      }

      describe("updating the callback status") {
        val allowedCallbackStatusUpdates = Table(
          ("initial", "update"),
          (Callback.Pending, Callback.Pending),
          (Callback.Pending, Callback.Succeeded),
          (Callback.Pending, Callback.Failed),
          (Callback.Succeeded, Callback.Succeeded),
          (Callback.Failed, Callback.Failed),
        )

        it("updates the status of a callback") {
          forAll(allowedCallbackStatusUpdates) {
            case (
                initialStatus: Callback.CallbackStatus,
                updatedStatus: Callback.CallbackStatus) =>
              val ingest = createIngestWith(
                callback = Some(
                  Callback(
                    uri = new URI("https://example.org/callback"),
                    status = initialStatus
                  ))
              )

              val update = createIngestCallbackStatusUpdateWith(
                id = ingest.id,
                callbackStatus = updatedStatus
              )

              withIngestTrackerFixtures(initialIngests = Seq(ingest)) {
                tracker =>
                  val result = tracker.update(update)
                  val ingest = result.right.value.identifiedT
                  ingest.callback.get.status shouldBe updatedStatus
              }
          }
        }

        val disallowedCallbackStatusUpdates = Table(
          ("initial", "update"),
          (Callback.Succeeded, Callback.Pending),
          (Callback.Succeeded, Callback.Failed),
          (Callback.Failed, Callback.Pending),
          (Callback.Failed, Callback.Succeeded),
        )

        it("does not allow the callback status to go backwards") {
          forAll(disallowedCallbackStatusUpdates) {
            case (
                initialStatus: Callback.CallbackStatus,
                updatedStatus: Callback.CallbackStatus) =>
              val ingest = createIngestWith(
                callback = Some(
                  Callback(
                    uri = new URI("https://example.org/callback"),
                    status = initialStatus
                  ))
              )

              val update = createIngestCallbackStatusUpdateWith(
                id = ingest.id,
                callbackStatus = updatedStatus
              )

              withIngestTrackerFixtures(initialIngests = Seq(ingest)) {
                tracker =>
                  val result = tracker.update(update)
                  result.left.value shouldBe IngestCallbackStatusGoingBackwards(
                    initialStatus,
                    updatedStatus)
              }
          }
        }

        it("errors if the ingest does not have a callback") {
          val ingest = createIngestWith(
            callback = None
          )
          val update = createIngestCallbackStatusUpdateWith(
            id = ingest.id
          )

          withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
            val result = tracker.update(update)
            result.left.value shouldBe a[NoCallbackOnIngest]
          }
        }
      }

      it("errors if there is no existing ingest with this ID") {
        val update = createIngestCallbackStatusUpdate

        withIngestTrackerFixtures() { tracker =>
          tracker
            .update(update)
            .left
            .value shouldBe a[UpdateNonExistentIngestError]
        }
      }
    }

    it("wraps an error from the underlying update() method") {
      val ingest = createIngest
      val update = createIngestCallbackStatusUpdate

      withBrokenUpdateStoreContext { implicit context =>
        withIngestTracker(initialIngests = Seq(ingest)) { tracker =>
          tracker.update(update).left.value shouldBe a[IngestTrackerStoreError]
        }
      }
    }
  }

  describe("listByBagId()") {
    it("returns an empty list if there aren't any matching ingests") {
      withIngestTrackerFixtures() { tracker =>
        tracker.listByBagId(createBagId).right.value shouldBe Seq.empty
      }
    }

    it("finds a single matching ingest") {
      val bagId = createBagId

      val ingest = createIngestWith(
        maybeBag = Some(bagId)
      )

      withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
        tracker.listByBagId(bagId).right.value shouldBe Seq(ingest)
      }
    }

    it("ignores ingests with a different bag ID or not bag ID") {
      val bagId = createBagId

      val matchingIngests = (1 to 3).map { _ =>
        createIngestWith(
          maybeBag = Some(bagId)
        )
      }

      val initialIngests = matchingIngests ++ Seq(
        createIngestWith(maybeBag = Some(createBagId)),
        createIngestWith(maybeBag = None)
      )

      withIngestTrackerFixtures(initialIngests) { tracker =>
        tracker
          .listByBagId(bagId)
          .right
          .value should contain theSameElementsAs matchingIngests
      }
    }
  }
}
