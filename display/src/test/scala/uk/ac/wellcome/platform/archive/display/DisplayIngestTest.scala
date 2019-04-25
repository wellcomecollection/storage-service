package uk.ac.wellcome.platform.archive.display

import java.net.{URI, URL}
import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.generators.BagIdGenerators
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.ObjectLocation

class DisplayIngestTest
    extends FunSpec
    with Matchers
    with BagIdGenerators
    with TimeTestFixture {

  private val id = createIngestID
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

    val displayIngest = ResponseDisplayIngest(ingest, contextUrl)

    displayIngest.id shouldBe id.underlying
    displayIngest.sourceLocation shouldBe DisplayLocation(
      StandardDisplayProvider,
      bucket = "bukkit",
      path = "key.txt")
    displayIngest.callback shouldBe Some(
      DisplayCallback(callbackUrl, Some(displayIngest.callback.get.status.get)))
    displayIngest.space shouldBe DisplayStorageSpace(spaceId)
    displayIngest.status shouldBe DisplayStatus("processing")
    displayIngest.bag shouldBe Some(
      IngestDisplayBag(s"${bagId.space}/${bagId.externalIdentifier}"))
    displayIngest.createdDate shouldBe createdDate
    displayIngest.lastModifiedDate shouldBe modifiedDate
    displayIngest.events shouldBe List(
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

    ingest.id shouldBe a[IngestID]
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
