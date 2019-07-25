package uk.ac.wellcome.platform.archive.common.ingests.services

import java.net.URI

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, Ingest}

class IngestStatesTest
  extends FunSpec
    with Matchers
    with IngestGenerators
    with TryValues
    with TableDrivenPropertyChecks {

  describe("handling an IngestEventUpdate") {
    it("adds an event to an ingest") {
      val ingest = createIngestWith(events = List.empty)

      val event = createIngestEvent
      val update = createIngestEventUpdateWith(
        id = ingest.id,
        events = List(event)
      )

      val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
      updatedIngest.events shouldBe Seq(event)
    }

    it("adds multiple events to an ingest") {
      val ingest = createIngestWith(events = List.empty)

      val events =
        List(createIngestEvent, createIngestEvent, createIngestEvent)
      val update = createIngestEventUpdateWith(
        id = ingest.id,
        events = events
      )

      val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
      updatedIngest.events shouldBe events
    }

    it("preserves the existing events on an ingest") {
      val existingEvents = List(createIngestEvent, createIngestEvent)
      val ingest = createIngestWith(events = existingEvents)

      val newEvents = List(createIngestEvent, createIngestEvent)
      val update = createIngestEventUpdateWith(
        id = ingest.id,
        events = newEvents
      )

      val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
      updatedIngest.events shouldBe existingEvents ++ newEvents
    }

    val eventStatusUpdates = Table(
      ("initial", "expected"),
      (Ingest.Accepted, Ingest.Processing),
      (Ingest.Processing, Ingest.Processing),
      (Ingest.Completed, Ingest.Completed),
      (Ingest.Failed, Ingest.Failed),
    )

    it("updates the status to Processing when it sees the first event update") {
      forAll(eventStatusUpdates) {
        case (initialStatus: Ingest.Status, expectedStatus: Ingest.Status) =>
          val ingest = createIngestWith(status = initialStatus)

          val update = createIngestEventUpdate

          val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
          updatedIngest.status shouldBe expectedStatus
      }
    }
  }

  describe("handling an IngestStatusUpdate") {
    describe("updating the events") {
      it("adds an event to an ingest") {
        val ingest = createIngestWith(events = List.empty)

        val event = createIngestEvent
        val update = createIngestStatusUpdateWith(
          id = ingest.id,
          events = List(event)
        )

        val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
        updatedIngest.events shouldBe Seq(event)
      }

      it("adds multiple events to an ingest") {
        val ingest = createIngestWith(events = List.empty)

        val events =
          List(createIngestEvent, createIngestEvent, createIngestEvent)
        val update = createIngestStatusUpdateWith(
          id = ingest.id,
          events = events
        )

        val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
        updatedIngest.events shouldBe events
      }

      it("preserves the existing events on an ingest") {
        val existingEvents = List(createIngestEvent, createIngestEvent)
        val ingest = createIngestWith(events = existingEvents)

        val newEvents = List(createIngestEvent, createIngestEvent)
        val update = createIngestStatusUpdateWith(
          id = ingest.id,
          events = newEvents
        )

        val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
        updatedIngest.events shouldBe existingEvents ++ newEvents
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

            val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
            updatedIngest.status shouldBe updatedStatus
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

            IngestStates.applyUpdate(ingest, update).failure.exception shouldBe a[IngestStatusGoingBackwardsException]
        }
      }
    }
  }

  describe("handling a CallbackStatusUpdate") {
    describe("updating the events") {
      it("adds an event to an ingest") {
        val ingest = createIngestWith(events = List.empty)

        val event = createIngestEvent
        val update = createIngestCallbackStatusUpdateWith(
          id = ingest.id,
          events = List(event)
        )

        val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
        updatedIngest.events shouldBe Seq(event)
      }

      it("adds multiple events to an ingest") {
        val ingest = createIngestWith(events = List.empty)

        val events =
          List(createIngestEvent, createIngestEvent, createIngestEvent)
        val update = createIngestCallbackStatusUpdateWith(
          id = ingest.id,
          events = events
        )

        val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
        updatedIngest.events shouldBe events
      }

      it("preserves the existing events on an ingest") {
        val existingEvents = List(createIngestEvent, createIngestEvent)
        val ingest = createIngestWith(events = existingEvents)

        val newEvents = List(createIngestEvent, createIngestEvent)
        val update = createIngestCallbackStatusUpdateWith(
          id = ingest.id,
          events = newEvents
        )

        val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
        updatedIngest.events shouldBe existingEvents ++ newEvents
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

            val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
            updatedIngest.callback.get.status shouldBe updatedStatus
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

            val err = IngestStates.applyUpdate(ingest, update).failure.exception

            err shouldBe a[CallbackStatusGoingBackwardsException]
        }
      }

      it("errors if the ingest does not have a callback") {
        val ingest = createIngestWith(
          callback = None
        )
        val update = createIngestCallbackStatusUpdateWith(
          id = ingest.id
        )

        val err = IngestStates.applyUpdate(ingest, update).failure.exception

        err shouldBe a[NoCallbackException]
      }
    }
  }
}
