package uk.ac.wellcome.platform.archive.common.generators

import java.net.URI
import java.time.Instant

import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Status
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.ObjectLocation

trait IngestGenerators extends BagIdGenerators {

  val storageLocation = StorageLocation(
    StandardStorageProvider,
    ObjectLocation(randomAlphanumeric(), randomAlphanumeric()))

  def createIngest: Ingest = createIngestWith()

  val testCallbackUri =
    new URI("http://www.wellcomecollection.org/callback/ok")

  def createIngestWith(id: IngestID = createIngestID,
                       sourceLocation: StorageLocation = storageLocation,
                       callback: Option[Callback] = Some(createCallback()),
                       space: Namespace = createSpace,
                       status: Status = Ingest.Accepted,
                       maybeBag: Option[BagId] = None,
                       createdDate: Instant = Instant.now,
                       events: List[IngestEvent] = List.empty): Ingest =
    Ingest(
      id = id,
      sourceLocation = sourceLocation,
      callback = callback,
      space = space,
      status = status,
      bag = maybeBag,
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
                                   maybeBag: Option[BagId] = Some(createBagId),
                                   events: Seq[IngestEvent] = List(
                                     createIngestEvent)): IngestStatusUpdate =
    IngestStatusUpdate(
      id = id,
      status = status,
      affectedBag = maybeBag,
      events = events
    )

  def createSpace = Namespace(randomAlphanumeric())

  def createCallback(): Callback = createCallbackWith()

  def createCallbackWith(
    uri: URI = testCallbackUri,
    status: Callback.CallbackStatus = Callback.Pending): Callback =
    Callback(uri = uri, status = status)

}
