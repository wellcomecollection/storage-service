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
      randomAlphanumericWithLength()
    )
  )

  def createIngest: Ingest = createIngestWith()

  val testCallbackUri =
    new URI("http://www.wellcomecollection.org/callback/ok")

  private def maybeVersion: Option[BagVersion] =
    if (Random.nextBoolean()) {
      Some(createBagVersion)
    } else {
      None
    }

  def createIngestWith(
    id: IngestID = createIngestID,
    ingestType: IngestType = CreateIngestType,
    sourceLocation: StorageLocation = storageLocation,
    callback: Option[Callback] = Some(createCallback()),
    space: StorageSpace = createStorageSpace,
    status: Status = Ingest.Accepted,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: Option[BagVersion] = maybeVersion,
    createdDate: Instant = randomInstant,
    events: Seq[IngestEvent] = Seq.empty
  ): Ingest =
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
      events = events
    )

  def createIngestEvent: IngestEvent =
    createIngestEventWith()

  def createIngestEvents(count: Int): Seq[IngestEvent] =
    (1 to count)
      .map { _ =>
        createIngestEvent
      }
      .sortBy { _.createdDate }

  def createIngestEventWith(
    description: String = randomAlphanumeric,
    createdDate: Instant =
      Instant.now().plusSeconds(randomInt(from = 0, to = 30))
  ): IngestEvent =
    IngestEvent(
      description = description,
      createdDate = createdDate
    )

  def createIngestEventUpdateWith(
    id: IngestID,
    events: Seq[IngestEvent] = List(createIngestEvent)
  ): IngestEventUpdate =
    IngestEventUpdate(
      id = id,
      events = events
    )

  def createIngestEventUpdate: IngestEventUpdate =
    createIngestEventUpdateWith(id = createIngestID)

  def createIngestStatusUpdateWith(
    id: IngestID = createIngestID,
    status: Status = Ingest.Accepted,
    events: Seq[IngestEvent] = List(createIngestEvent)
  ): IngestStatusUpdate =
    IngestStatusUpdate(
      id = id,
      status = status,
      events = events
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

  def createIngestVersionUpdateWith(
    id: IngestID = createIngestID,
    events: Seq[IngestEvent] = Seq(createIngestEvent),
    version: BagVersion = createBagVersion
  ): IngestVersionUpdate =
    IngestVersionUpdate(
      id = id,
      events = events,
      version = version
    )

  def createCallback(): Callback = createCallbackWith()

  def createCallbackWith(
    uri: URI = testCallbackUri,
    status: Callback.CallbackStatus = Callback.Pending
  ): Callback =
    Callback(uri = uri, status = status)

}
