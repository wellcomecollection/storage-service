package uk.ac.wellcome.platform.archive.display

import java.net.{URI, URL}
import java.time.Instant
import java.util.UUID

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.BagIdGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.ObjectLocation

class DisplayIngestTest
    extends FunSpec
    with Matchers
    with BagIdGenerators
    with TimeTestFixture {

  private val id = UUID.randomUUID()
  private val callbackUrl = "http://www.example.com/callback"
  private val spaceId = "space-id"
  private val createdDate = "2018-10-10T09:38:55.321Z"
  private val modifiedDate = "2018-10-10T09:38:55.322Z"
  private val eventDate = "2018-10-10T09:38:55.323Z"
  private val eventDescription = "Event description"
  private val contextUrl = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json")

  it("creates a DisplayIngest from Ingest") {
    val bagId = createBagId
    val ingest: Ingest = Ingest(
      id,
      StorageLocation(
        StandardStorageProvider,
        ObjectLocation("bukkit", "key.txt")),
      Namespace(spaceId),
      Some(Callback(new URI(callbackUrl))),
      Ingest.Processing,
      Some(bagId),
      Instant.parse(createdDate),
      Instant.parse(modifiedDate),
      List(IngestEvent(eventDescription, Instant.parse(eventDate)))
    )

    val ingest = ResponseDisplayIngest(ingest, contextUrl)

    ingest.id shouldBe id
    ingest.sourceLocation shouldBe DisplayLocation(
      StandardDisplayProvider,
      bucket = "bukkit",
      path = "key.txt")
    ingest.callback shouldBe Some(
      DisplayCallback(callbackUrl, Some(ingest.callback.get.status.get)))
    ingest.space shouldBe DisplayStorageSpace(spaceId)
    ingest.status shouldBe DisplayStatus("processing")
    ingest.bag shouldBe Some(
      IngestDisplayBag(s"${bagId.space}/${bagId.externalIdentifier}"))
    ingest.createdDate shouldBe createdDate
    ingest.lastModifiedDate shouldBe modifiedDate
    ingest.events shouldBe List(
      DisplayIngestEvent(eventDescription, eventDate))
  }

  it("transforms itself into a ingest") {
    val displayProvider = InfrequentAccessDisplayProvider
    val bucket = "ingest-bucket"
    val path = "bag.zip"
    val ingestCreateRequest = RequestDisplayIngest(
      DisplayLocation(displayProvider, bucket, path),
      Some(
        DisplayCallback("http://www.wellcomecollection.org/callback/ok", None)),
      CreateDisplayIngestType,
      DisplayStorageSpace("space-id")
    )

    val ingest = ingestCreateRequest.toIngest

    ingest.id shouldBe a[UUID]
    ingest.sourceLocation shouldBe StorageLocation(
      InfrequentAccessStorageProvider,
      ObjectLocation(bucket, path))
    ingest.callback shouldBe Some(
      Callback(URI.create(ingestCreateRequest.callback.get.url)))
    ingest.status shouldBe Ingest.Accepted
    assertRecent(ingest.createdDate)
    assertRecent(ingest.lastModifiedDate)
    ingest.events shouldBe List.empty
  }
}
