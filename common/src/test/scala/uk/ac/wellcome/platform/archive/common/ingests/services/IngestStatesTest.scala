package uk.ac.wellcome.platform.archive.common.ingests.services

import java.net.URI

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models._

sealed trait IngestUpdateTestCases[UpdateType <: IngestUpdate]
    extends FunSpec
    with Matchers
    with IngestGenerators
    with TryValues {
  def createUpdateWith(id: IngestID, events: Seq[IngestEvent]): UpdateType

  def createInitialIngestWith(events: Seq[IngestEvent]): Ingest =
    createIngestWith(events = events)

  describe("updating the events") {
    it("adds an event") {
      val ingest = createInitialIngestWith(events = List.empty)

      val event = createIngestEvent
      val update = createUpdateWith(
        id = ingest.id,
        events = List(event)
      )

      val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
      updatedIngest.events shouldBe Seq(event)
    }

    it("adds multiple events") {
      val ingest = createInitialIngestWith(events = List.empty)

      val events =
        List(createIngestEvent, createIngestEvent, createIngestEvent)
      val update = createUpdateWith(
        id = ingest.id,
        events = events
      )

      val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
      updatedIngest.events shouldBe events
    }

    it("preserves the existing events") {
      val existingEvents = List(createIngestEvent, createIngestEvent)
      val ingest = createInitialIngestWith(events = existingEvents)

      val newEvents = List(createIngestEvent, createIngestEvent)
      val update = createUpdateWith(
        id = ingest.id,
        events = newEvents
      )

      val updatedIngest = IngestStates.applyUpdate(ingest, update).success.value
      updatedIngest.events shouldBe existingEvents ++ newEvents
    }
  }
}

class IngestEventUpdateTest
    extends IngestUpdateTestCases[IngestEventUpdate]
    with TableDrivenPropertyChecks {
  override def createUpdateWith(
    id: IngestID,
    events: Seq[IngestEvent]
  ): IngestEventUpdate =
    createIngestEventUpdateWith(id = id, events = events)

  val eventStatusUpdates = Table(
    ("initial", "expected"),
    (Ingest.Accepted, Ingest.Processing),
    (Ingest.Processing, Ingest.Processing),
    (Ingest.Completed, Ingest.Completed),
    (Ingest.Failed, Ingest.Failed)
  )

  it("updates the status to Processing when it sees the first event update") {
    forAll(eventStatusUpdates) {
      case (initialStatus: Ingest.Status, expectedStatus: Ingest.Status) =>
        val ingest = createIngestWith(status = initialStatus)

        val update = createIngestEventUpdate

        val updatedIngest =
          IngestStates.applyUpdate(ingest, update).success.value
        updatedIngest.status shouldBe expectedStatus
    }
  }
}

class IngestStatusUpdateTest
    extends IngestUpdateTestCases[IngestStatusUpdate]
    with TableDrivenPropertyChecks {
  override def createUpdateWith(
    id: IngestID,
    events: Seq[IngestEvent]
  ): IngestStatusUpdate =
    createIngestStatusUpdateWith(id = id, events = events)

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

          val updatedIngest =
            IngestStates.applyUpdate(ingest, update).success.value
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

          IngestStates
            .applyUpdate(ingest, update)
            .failure
            .exception shouldBe a[IngestStatusGoingBackwardsException]
      }
    }
  }
}

class IngestCallbackStatusUpdateTest
    extends IngestUpdateTestCases[IngestCallbackStatusUpdate]
    with TableDrivenPropertyChecks {
  override def createUpdateWith(
    id: IngestID,
    events: Seq[IngestEvent]
  ): IngestCallbackStatusUpdate =
    createIngestCallbackStatusUpdateWith(id = id, events = events)

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

          val updatedIngest =
            IngestStates.applyUpdate(ingest, update).success.value
          updatedIngest.callback.get.status shouldBe updatedStatus
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

class IngestVersionUpdateTest
    extends IngestUpdateTestCases[IngestVersionUpdate] {
  override def createUpdateWith(
    id: IngestID,
    events: Seq[IngestEvent]
  ): IngestVersionUpdate =
    createIngestVersionUpdateWith(id = id, events = events)

  override def createInitialIngestWith(events: Seq[IngestEvent]): Ingest =
    createIngestWith(
      events = events,
      version = None
    )

  describe("updating the version") {
    it("sets the version if there isn't one already") {
      val ingest = createIngestWith(
        version = None
      )
      val update = createIngestVersionUpdateWith(
        id = ingest.id,
        version = BagVersion(1)
      )

      val updatedIngest =
        IngestStates.applyUpdate(ingest, update).success.value
      updatedIngest.version shouldBe Some(BagVersion(1))
    }

    it("does nothing if the version is already set") {
      val ingest = createIngestWith(
        version = Some(BagVersion(1))
      )
      val update = createIngestVersionUpdateWith(
        id = ingest.id,
        version = BagVersion(1)
      )

      val updatedIngest =
        IngestStates.applyUpdate(ingest, update).success.value
      updatedIngest.version shouldBe Some(BagVersion(1))
    }

    it("errors if the existing version is different") {
      val ingest = createIngestWith(
        version = Some(BagVersion(1))
      )
      val update = createIngestVersionUpdateWith(
        id = ingest.id,
        version = BagVersion(2)
      )

      val err = IngestStates.applyUpdate(ingest, update).failure.exception
      err shouldBe a[MismatchedVersionUpdateException]
    }
  }
}
