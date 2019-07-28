package uk.ac.wellcome.platform.archive.common.generators

import java.net.URI
import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Status
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.Random

trait IngestGenerators extends BagIdGenerators {

  val storageLocation = StorageLocation(
    StandardStorageProvider,
    ObjectLocation(
      randomAlphanumericWithLength(),
      randomAlphanumericWithLength()))

  def createIngest: Ingest = createIngestWith()

  val testCallbackUri =
    new URI("http://www.wellcomecollection.org/callback/ok")

  private def maybeVersion: Option[BagVersion] =
    if (Random.nextBoolean()) {
      Some(BagVersion(Random.nextInt))
    } else {
      None
    }

  private def maybeModifiedDate: Option[Instant] =
    if (Random.nextBoolean()) {
      Some(randomInstant)
    } else {
      None
    }

  def createIngestWith(id: IngestID = createIngestID,
                       ingestType: IngestType = CreateIngestType,
                       sourceLocation: StorageLocation = storageLocation,
                       callback: Option[Callback] = Some(createCallback()),
                       space: StorageSpace = createStorageSpace,
                       status: Status = Ingest.Accepted,
                       externalIdentifier: ExternalIdentifier =
                         createExternalIdentifier,
                       version: Option[BagVersion] = maybeVersion,
                       createdDate: Instant = randomInstant,
                       lastModifiedDate: Option[Instant] = maybeModifiedDate,
                       events: Seq[IngestEvent] = Seq.empty): Ingest =
    Ingest(
      id = id,
      ingestType = ingestType,
      sourceLocation = sourceLocation,
      callback = callback,
      space = space,
      status = status,
      externalIdentifier = externalIdentifier,
      version = version,
      createdDate = createdDate,
      lastModifiedDate = lastModifiedDate,
      events = events
    )

  def createIngestEvent: IngestEvent =
    IngestEvent(randomAlphanumeric)

  def createIngestEventUpdateWith(id: IngestID,
                                  events: List[IngestEvent] = List(
                                    createIngestEvent)): IngestEventUpdate =
    IngestEventUpdate(
      id = id,
      events = events
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

  def createIngestVersionUpdateWith(
    id: IngestID = createIngestID,
    events: Seq[IngestEvent] = Seq(createIngestEvent),
    version: BagVersion = BagVersion(Random.nextInt)
  ): IngestVersionUpdate =
    IngestVersionUpdate(
      id = id,
      events = events,
      version = version
    )

  def createIngestCallbackStatusUpdateWith(
    id: IngestID = createIngestID,
    callbackStatus: Callback.CallbackStatus = Callback.Pending,
    events: Seq[IngestEvent] = Seq.empty
  ): IngestCallbackStatusUpdate =
    IngestCallbackStatusUpdate(
      id = id,
      callbackStatus = callbackStatus,
      events = events
    )

  def createIngestCallbackStatusUpdate: IngestCallbackStatusUpdate =
    createIngestCallbackStatusUpdateWith()

  def createIngestStatusUpdate: IngestStatusUpdate =
    createIngestStatusUpdateWith()

  def createCallback(): Callback = createCallbackWith()

  def createCallbackWith(
    uri: URI = testCallbackUri,
    status: Callback.CallbackStatus = Callback.Pending): Callback =
    Callback(uri = uri, status = status)

}
