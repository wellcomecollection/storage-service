package uk.ac.wellcome.platform.archive.common.ingests.tracker

import java.net.URI
import java.time.Instant

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Callback,
  Ingest,
  IngestEvent
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
        tracker.init(ingest).right.value shouldBe Identified(
          Version(ingest.id, 0),
          ingest
        )
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
      withContext { implicit context =>
        withBrokenUnderlyingInitTracker { tracker =>
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
        val retrievedResult = tracker.get(ingest.id).right.value

        retrievedResult.id shouldBe Version(ingest.id, 0)
        assertIngestsEqual(retrievedResult.identifiedT, ingest)
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
          assertIngestEventSeqEqual(
            result.right.value.identifiedT.events,
            Seq(event)
          )

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          assertIngestEventSeqEqual(
            storedIngest.events,
            Seq(event)
          )
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
          assertIngestEventSeqEqual(
            result.right.value.identifiedT.events,
            events
          )

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          assertIngestEventSeqEqual(
            storedIngest.events,
            events
          )
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
          assertIngestEventSeqEqual(
            result.right.value.identifiedT.events,
            existingEvents ++ newEvents
          )

          val storedIngest = tracker.get(ingest.id).right.value.identifiedT
          assertIngestEventSeqEqual(
            storedIngest.events,
            existingEvents ++ newEvents
          )
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
            assertIngestEventSeqEqual(
              result.right.value.identifiedT.events,
              Seq(event)
            )

            val storedIngest = tracker.get(ingest.id).right.value.identifiedT
            assertIngestEventSeqEqual(
              storedIngest.events,
              Seq(event)
            )
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
            assertIngestEventSeqEqual(
              result.right.value.identifiedT.events,
              events
            )

            val storedIngest = tracker.get(ingest.id).right.value.identifiedT
            assertIngestEventSeqEqual(
              storedIngest.events,
              events
            )
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
            assertIngestEventSeqEqual(
              result.right.value.identifiedT.events,
              existingEvents ++ newEvents
            )

            val storedIngest = tracker.get(ingest.id).right.value.identifiedT
            assertIngestEventSeqEqual(
              storedIngest.events,
              existingEvents ++ newEvents
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
          (Ingest.Failed, Ingest.Failed)
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
          (Ingest.Processing, Ingest.Accepted)
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
            assertIngestEventSeqEqual(
              result.right.value.identifiedT.events,
              Seq(event)
            )

            val storedIngest = tracker.get(ingest.id).right.value.identifiedT
            assertIngestEventSeqEqual(
              storedIngest.events,
              Seq(event)
            )
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
            assertIngestEventSeqEqual(
              result.right.value.identifiedT.events,
              events
            )

            val storedIngest = tracker.get(ingest.id).right.value.identifiedT
            assertIngestEventSeqEqual(
              storedIngest.events,
              events
            )
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
            assertIngestEventSeqEqual(
              result.right.value.identifiedT.events,
              existingEvents ++ newEvents
            )

            val storedIngest = tracker.get(ingest.id).right.value.identifiedT
            assertIngestEventSeqEqual(
              storedIngest.events,
              existingEvents ++ newEvents
            )
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
                  result.left.value shouldBe IngestCallbackStatusGoingBackwards(
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
      val update = createIngestCallbackStatusUpdate

      withContext { implicit context =>
        withBrokenUnderlyingUpdateTracker { tracker =>
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
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val ingest = createIngestWith(
        space = space,
        externalIdentifier = externalIdentifier
      )

      val bagId = createBagIdWith(
        space = space,
        externalIdentifier = externalIdentifier
      )

      withIngestTrackerFixtures(initialIngests = Seq(ingest)) { tracker =>
        assertIngestSeqEqual(
          tracker.listByBagId(bagId).right.value,
          Seq(ingest)
        )
      }
    }

    it("ignores ingests with a different storage space or externalIdentifier") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val matchingIngests = (1 to 3).map { t =>
        createIngestWith(
          space = space,
          externalIdentifier = externalIdentifier,
          createdDate = Instant.ofEpochSecond(t)
        )
      }.reverse

      val initialIngests = matchingIngests ++ Seq(
        createIngestWith(space = space),
        createIngestWith(externalIdentifier = externalIdentifier),
        createIngest
      )

      val bagId = createBagIdWith(
        space = space,
        externalIdentifier = externalIdentifier
      )

      withIngestTrackerFixtures(initialIngests) { tracker =>
        assertIngestSeqEqual(
          tracker
            .listByBagId(bagId)
            .right
            .value,
          matchingIngests
        )
      }
    }

    it("sorts ingests by creation date") {
      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier

      val initialIngests = (1 to 5).map { _ =>
        createIngestWith(
          space = space,
          externalIdentifier = externalIdentifier,
          createdDate = randomInstant
        )
      }

      val bagId = createBagIdWith(
        space = space,
        externalIdentifier = externalIdentifier
      )

      withIngestTrackerFixtures(initialIngests) { tracker =>
        assertIngestSeqEqual(
          tracker
            .listByBagId(bagId)
            .right
            .value,
          initialIngests.sortBy { _.createdDate }.reverse
        )
      }
    }
  }

  protected def assertIngestsEqual(ingest1: Ingest, ingest2: Ingest): Assertion =
    ingest1 shouldBe ingest2

  def assertIngestSeqEqual(seq1: Seq[Ingest], seq2: Seq[Ingest]): Seq[Assertion] = {
    seq1.size shouldBe seq2.size

    seq1.zip(seq2).map { case (ingest1, ingest2) =>
      assertIngestsEqual(ingest1, ingest2)
    }
  }

  protected def assertIngestEventsEqual(event1: IngestEvent, event2: IngestEvent): Assertion =
    event1 shouldBe event2

  def assertIngestEventSeqEqual(seq1: Seq[IngestEvent], seq2: Seq[IngestEvent]): Seq[Assertion] = {
    seq1.size shouldBe seq2.size

    seq1.zip(seq2).map { case (event1, event2) =>
      assertIngestEventsEqual(event1, event2)
    }
  }
}
