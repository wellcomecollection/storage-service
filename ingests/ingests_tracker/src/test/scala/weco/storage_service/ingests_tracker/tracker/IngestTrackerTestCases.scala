package weco.storage_service.ingests_tracker.tracker

import java.net.URI

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models.{Callback, Ingest}
import weco.storage._

trait IngestTrackerTestCases[Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with IngestGenerators
    with TableDrivenPropertyChecks {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withIngestTracker[R](initialIngests: Seq[Ingest] = Seq.empty)(
    testWith: TestWith[IngestTracker, R]
  )(implicit context: Context): R

  private def withIngestTrackerFixtures[R](
    initialIngests: Seq[Ingest] = Seq.empty
  )(testWith: TestWith[IngestTracker, R]): R =
    withContext { implicit context =>
      withIngestTracker(initialIngests) { tracker =>
        testWith(tracker)
      }
    }

  def withBrokenUnderlyingInitTracker[R](testWith: TestWith[IngestTracker, R])(
    implicit context: Context
  ): R
  def withBrokenUnderlyingGetTracker[R](testWith: TestWith[IngestTracker, R])(
    implicit context: Context
  ): R
  def withBrokenUnderlyingUpdateTracker[R](
    testWith: TestWith[IngestTracker, R]
  )(implicit context: Context): R

  describe("init()") {
    it("creates an ingest") {
      withIngestTrackerFixtures() { tracker =>
        val ingest = createIngest
        tracker.init(ingest).value shouldBe Identified(
          Version(ingest.id, 0),
          ingest
        )
      }
    }

    it("allows calling init() twice with the same value") {
      withIngestTrackerFixtures() { tracker =>
        val ingest = createIngest
        tracker.init(ingest) shouldBe a[Right[_, _]]
        tracker.init(ingest) shouldBe a[Right[_, _]]
      }
    }

    it("doesn't allow calling init() twice with different values") {
      withIngestTrackerFixtures() { tracker =>
        val ingest1 = createIngest
        val ingest2 = createIngestWith(id = ingest1.id)
        tracker.init(ingest1) shouldBe a[Right[_, _]]
        tracker.init(ingest2).left.value shouldBe a[IngestAlreadyExistsError]
      }
    }

    it("wraps an init() error from the underlying store") {
      withContext { implicit context =>
        withBrokenUnderlyingInitTracker { tracker =>
          tracker
            .init(createIngest)
            .left
            .value shouldBe a[IngestStoreError]
        }
      }
    }
  }

  describe("get()") {
    it("finds an ingest") {
      val ingest = createIngest

      withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
        val retrievedResult = tracker.get(ingest.id).value

        retrievedResult.id shouldBe Version(ingest.id, 0)
        retrievedResult.identifiedT shouldBe ingest
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
      withContext { implicit context =>
        withBrokenUnderlyingGetTracker { tracker =>
          tracker
            .get(createIngestID)
            .left
            .value shouldBe a[IngestStoreError]
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
          result.value.identifiedT.events shouldBe Seq(event)

          val storedIngest = tracker.get(ingest.id).value.identifiedT
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
          result.value.identifiedT.events shouldBe events

          val storedIngest = tracker.get(ingest.id).value.identifiedT
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
          result.value.identifiedT.events shouldBe existingEvents ++ newEvents

          val storedIngest = tracker.get(ingest.id).value.identifiedT
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
            result.value.identifiedT.events shouldBe Seq(event)

            val storedIngest = tracker.get(ingest.id).value.identifiedT
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
            result.value.identifiedT.events shouldBe events

            val storedIngest = tracker.get(ingest.id).value.identifiedT
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
            result.value.identifiedT.events shouldBe existingEvents ++ newEvents

            val storedIngest = tracker.get(ingest.id).value.identifiedT
            storedIngest.events shouldBe existingEvents ++ newEvents
          }
        }
      }

      describe("updating the status") {
        val allowedStatusUpdates = Table(
          ("initial", "update"),
          (Ingest.Accepted, Ingest.Accepted),
          (Ingest.Accepted, Ingest.Processing),
          (Ingest.Accepted, Ingest.Failed),
          (Ingest.Accepted, Ingest.Succeeded),
          (Ingest.Processing, Ingest.Processing),
          (Ingest.Processing, Ingest.Failed),
          (Ingest.Processing, Ingest.Succeeded),
          (Ingest.Failed, Ingest.Failed),
          (Ingest.Failed, Ingest.Succeeded),
          (Ingest.Succeeded, Ingest.Succeeded)
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
                  result.value.identifiedT.status shouldBe updatedStatus
              }
          }
        }

        val disallowedStatusUpdates = Table(
          ("initial", "update"),
          (Ingest.Processing, Ingest.Accepted),
          (Ingest.Failed, Ingest.Accepted),
          (Ingest.Failed, Ingest.Processing),
          (Ingest.Succeeded, Ingest.Accepted),
          (Ingest.Succeeded, Ingest.Processing),
          (Ingest.Succeeded, Ingest.Failed)
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
                  result.left.value shouldBe a[IngestStatusGoingBackwardsError]
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
            result.value.identifiedT.events shouldBe Seq(event)

            val storedIngest = tracker.get(ingest.id).value.identifiedT
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
            result.value.identifiedT.events shouldBe events

            val storedIngest = tracker.get(ingest.id).value.identifiedT
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
            result.value.identifiedT.events shouldBe existingEvents ++ newEvents

            val storedIngest = tracker.get(ingest.id).value.identifiedT
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
          (Callback.Failed, Callback.Failed)
        )

        it("updates the status of a callback") {
          forAll(allowedCallbackStatusUpdates) {
            case (
                initialStatus: Callback.CallbackStatus,
                updatedStatus: Callback.CallbackStatus
                ) =>
              val ingest = createIngestWith(
                callback = Some(
                  Callback(
                    uri = new URI("https://example.org/callback"),
                    status = initialStatus
                  )
                )
              )

              val update = createIngestCallbackStatusUpdateWith(
                id = ingest.id,
                callbackStatus = updatedStatus
              )

              withIngestTrackerFixtures(initialIngests = Seq(ingest)) {
                tracker =>
                  val result = tracker.update(update)
                  val ingest = result.value.identifiedT
                  ingest.callback.get.status shouldBe updatedStatus
              }
          }
        }

        val disallowedCallbackStatusUpdates = Table(
          ("initial", "update"),
          (Callback.Succeeded, Callback.Pending),
          (Callback.Succeeded, Callback.Failed),
          (Callback.Failed, Callback.Pending),
          (Callback.Failed, Callback.Succeeded)
        )

        it("does not allow the callback status to go backwards") {
          forAll(disallowedCallbackStatusUpdates) {
            case (
                initialStatus: Callback.CallbackStatus,
                updatedStatus: Callback.CallbackStatus
                ) =>
              val ingest = createIngestWith(
                callback = Some(
                  Callback(
                    uri = new URI("https://example.org/callback"),
                    status = initialStatus
                  )
                )
              )

              val update = createIngestCallbackStatusUpdateWith(
                id = ingest.id,
                callbackStatus = updatedStatus
              )

              withIngestTrackerFixtures(initialIngests = Seq(ingest)) {
                tracker =>
                  val result = tracker.update(update)
                  result.left.value shouldBe IngestCallbackStatusGoingBackwardsError(
                    initialStatus,
                    updatedStatus
                  )
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
            result.left.value shouldBe a[NoCallbackOnIngestError]
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
      val update = createIngestCallbackStatusUpdate

      withContext { implicit context =>
        withBrokenUnderlyingUpdateTracker { tracker =>
          tracker.update(update).left.value shouldBe a[IngestStoreError]
        }
      }
    }
  }
}
