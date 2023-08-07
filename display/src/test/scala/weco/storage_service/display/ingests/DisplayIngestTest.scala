package weco.storage_service.display.ingests

import java.net.URI
import java.time.Instant
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.generators.{BagIdGenerators, IngestGenerators}
import weco.storage_service.ingests.models._
import weco.storage_service.storage.models.StorageSpace
import weco.storage_service.display._
import weco.storage.providers.s3.S3ObjectLocation
import weco.fixtures.TimeAssertions

class DisplayIngestTest
    extends AnyFunSpec
    with Matchers
    with BagIdGenerators
    with TimeAssertions
    with IngestGenerators {

  private val id = createIngestID
  private val callbackUrl = "http://www.example.com/callback"
  private val spaceId = "space-id"
  private val createdDate = "2018-10-10T09:38:55.321Z"
  private val eventDate = "2018-10-10T09:38:55.323Z"
  private val eventDescription = "Event description"

  describe("ResponseDisplayIngest") {
    it("creates a DisplayIngest from Ingest") {
      val externalIdentifier = createExternalIdentifier
      val ingest: Ingest = Ingest(
        id = id,
        ingestType = CreateIngestType,
        sourceLocation = S3SourceLocation(
          location = S3ObjectLocation("bukkit", "mybag.tar.gz")
        ),
        space = StorageSpace(spaceId),
        callback = Some(Callback(new URI(callbackUrl))),
        status = Ingest.Processing,
        externalIdentifier = externalIdentifier,
        createdDate = Instant.parse(createdDate),
        events = List(IngestEvent(eventDescription, Instant.parse(eventDate)))
      )

      val displayIngest = ResponseDisplayIngest(ingest)

      displayIngest.id shouldBe id.uuid
      displayIngest.sourceLocation shouldBe DisplayLocation(
        DisplayProvider(id = "amazon-s3"),
        bucket = "bukkit",
        path = "mybag.tar.gz"
      )
      displayIngest.callback shouldBe Some(
        DisplayCallback(
          url = callbackUrl,
          status = Some(displayIngest.callback.get.status.get)
        )
      )
      displayIngest.space shouldBe DisplayStorageSpace(spaceId)
      displayIngest.status shouldBe DisplayStatus("processing")
      displayIngest.bag.info.externalIdentifier shouldBe externalIdentifier
      displayIngest.createdDate shouldBe createdDate
      displayIngest.lastModifiedDate.get shouldBe eventDate
      displayIngest.events shouldBe List(
        DisplayIngestEvent(eventDescription, eventDate)
      )
    }

    it("sorts events by created date") {
      val events = Seq(1, 3, 2, 4, 5).map { i =>
        IngestEvent(
          description = s"Event $i",
          createdDate = Instant.ofEpochSecond(i)
        )
      }

      val ingest = createIngestWith(events = events)

      val displayIngest = ResponseDisplayIngest(ingest)

      displayIngest.events.map { _.description } shouldBe Seq(
        "Event 1",
        "Event 2",
        "Event 3",
        "Event 4",
        "Event 5"
      )
    }

    it("sets an ingestType of 'create'") {
      val ingest = createIngestWith(ingestType = CreateIngestType)

      val displayIngest = ResponseDisplayIngest(ingest)

      displayIngest.ingestType.id shouldBe "create"
    }

    it("sets an ingestType of 'update'") {
      val ingest = createIngestWith(ingestType = UpdateIngestType)

      val displayIngest = ResponseDisplayIngest(ingest)

      displayIngest.ingestType.id shouldBe "update"
    }
  }

  describe("RequestDisplayIngest") {
    it("transforms itself into a ingest") {
      val displayProvider = DisplayProvider(id = "amazon-s3")
      val bucket = "ingest-bucket"
      val path = "bag.tar.gz"

      val externalIdentifier = createExternalIdentifier

      val ingestCreateRequest = RequestDisplayIngest(
        sourceLocation = DisplayLocation(displayProvider, bucket, path),
        callback = Some(
          DisplayCallback(
            url = "http://www.wellcomecollection.org/callback/ok",
            status = None
          )
        ),
        bag = RequestDisplayBag(
          info = RequestDisplayBagInfo(
            externalIdentifier = externalIdentifier.underlying
          )
        ),
        ingestType = CreateDisplayIngestType,
        space = DisplayStorageSpace("space-id")
      )

      val ingest = ingestCreateRequest.toIngest

      ingest.id shouldBe a[IngestID]
      ingest.sourceLocation shouldBe S3SourceLocation(
        location = S3ObjectLocation(bucket, path)
      )
      ingest.callback shouldBe Some(
        Callback(URI.create(ingestCreateRequest.callback.get.url))
      )
      ingest.status shouldBe Ingest.Accepted
      ingest.externalIdentifier shouldBe externalIdentifier
      assertRecent(ingest.createdDate)
      ingest.events shouldBe empty
    }

    it("sets an ingest type of 'create'") {
      val displayRequest = createRequestDisplayIngestWith(
        ingestType = CreateDisplayIngestType
      )

      displayRequest.toIngest.ingestType shouldBe CreateIngestType
    }

    it("sets an ingest type of 'update'") {
      val displayRequest = createRequestDisplayIngestWith(
        ingestType = UpdateDisplayIngestType
      )

      displayRequest.toIngest.ingestType shouldBe UpdateIngestType
    }
  }

  def createRequestDisplayIngestWith(
    sourceLocation: DisplayLocation = DisplayLocation(
      provider = DisplayProvider(id = "aws-s3-ia"),
      bucket = createBucketName,
      path = randomAlphanumeric()
    ),
    callback: Option[DisplayCallback] = None,
    ingestType: DisplayIngestType = CreateDisplayIngestType,
    space: DisplayStorageSpace = DisplayStorageSpace(
      createStorageSpace.underlying
    )
  ): RequestDisplayIngest =
    RequestDisplayIngest(
      sourceLocation = sourceLocation,
      callback = callback,
      ingestType = ingestType,
      bag = RequestDisplayBag(
        info = RequestDisplayBagInfo(
          externalIdentifier = createExternalIdentifier.underlying
        )
      ),
      space = space
    )
}
