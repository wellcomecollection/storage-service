package uk.ac.wellcome.platform.archive.common.generators

import java.net.URI
import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Status
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

trait IngestGenerators extends BagIdGenerators {

  val storageLocation = StorageLocation(
    StandardStorageProvider,
    ObjectLocation(randomAlphanumeric(), randomAlphanumeric()))

  def createIngest: Ingest = createIngestWith()

  val testCallbackUri =
    new URI("http://www.wellcomecollection.org/callback/ok")

  def createIngestWith(id: IngestID = createIngestID,
                       ingestType: IngestType = CreateIngestType,
                       sourceLocation: StorageLocation = storageLocation,
                       callback: Option[Callback] = Some(createCallback()),
                       space: StorageSpace = createStorageSpace,
                       status: Status = Ingest.Accepted,
                       externalIdentifier: ExternalIdentifier = createExternalIdentifier,
                       createdDate: Instant = Instant.now,
                       events: Seq[IngestEvent] = Seq.empty): Ingest =
    Ingest(
      id = id,
      ingestType = ingestType,
      sourceLocation = sourceLocation,
      callback = callback,
      space = space,
      status = status,
      externalIdentifier = externalIdentifier,
      createdDate = createdDate,
      events = events
    )

  def createIngestEvent: IngestEvent =
    IngestEvent(randomAlphanumeric(15))

  def createIngestEventUpdateWith(id: IngestID): IngestEventUpdate =
    IngestEventUpdate(
      id = id,
      events = List(createIngestEvent)
    )

  def createIngestEventUpdate: IngestEventUpdate =
    createIngestEventUpdateWith(id = createIngestID)

  def createIngestStatusUpdateWith(id: IngestID = createIngestID,
                                   status: Status = Ingest.Accepted,
                                   events: Seq[IngestEvent] = List(
                                     createIngestEvent)): IngestStatusUpdate =
    IngestStatusUpdate(
      id = id,
      status = status,
      events = events
    )

  def createCallback(): Callback = createCallbackWith()

  def createCallbackWith(
    uri: URI = testCallbackUri,
    status: Callback.CallbackStatus = Callback.Pending): Callback =
    Callback(uri = uri, status = status)

}
