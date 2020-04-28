package uk.ac.wellcome.platform.archive.display.ingests

import java.net.{URI, URL}
import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  IngestGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.display._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class DisplayIngestTest
    extends FunSpec
    with Matchers
    with BagIdGenerators
    with TimeTestFixture
    with IngestGenerators
    with ObjectLocationGenerators {

  private val id = createIngestID
  private val callbackUrl = "http://www.example.com/callback"
  private val spaceId = "space-id"
  private val createdDate = "2018-10-10T09:38:55.321Z"
  private val eventDate = "2018-10-10T09:38:55.323Z"
  private val eventDescription = "Event description"
  private val contextUrl = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json"
  )

  describe("ResponseDisplayIngest") {
    it("creates a DisplayIngest from Ingest") {
      val externalIdentifier = createExternalIdentifier
      val ingest: Ingest = Ingest(
        id = id,
        ingestType = CreateIngestType,
        sourceLocation = SourceLocation(
          provider = StandardStorageProvider,
          location = ObjectLocation("bukkit", "key.txt")
        ),
        space = StorageSpace(spaceId),
        callback = Some(Callback(new URI(callbackUrl))),
        status = Ingest.Processing,
        externalIdentifier = externalIdentifier,
        createdDate = Instant.parse(createdDate),
        events = List(IngestEvent(eventDescription, Instant.parse(eventDate)))
      )

      val displayIngest = ResponseDisplayIngest(ingest, contextUrl)

      displayIngest.id shouldBe id.underlying
      displayIngest.sourceLocation shouldBe DisplayLocation(
        StandardDisplayProvider,
        bucket = "bukkit",
        path = "key.txt"
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

      val displayIngest = ResponseDisplayIngest(ingest, contextUrl)

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

      val displayIngest = ResponseDisplayIngest(ingest, contextUrl)

      displayIngest.ingestType.id shouldBe "create"
    }

    it("sets an ingestType of 'update'") {
      val ingest = createIngestWith(ingestType = UpdateIngestType)

      val displayIngest = ResponseDisplayIngest(ingest, contextUrl)

      displayIngest.ingestType.id shouldBe "update"
    }
  }

  describe("RequestDisplayIngest") {
    it("transforms itself into a ingest") {
      val displayProvider = InfrequentAccessDisplayProvider
      val bucket = "ingest-bucket"
      val path = "bag.zip"

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
      ingest.sourceLocation shouldBe SourceLocation(
        provider = InfrequentAccessStorageProvider,
        location = ObjectLocation(bucket, path)
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
      provider = InfrequentAccessDisplayProvider,
      bucket = randomAlphanumeric,
      path = randomAlphanumeric
    ),
    callback: Option[DisplayCallback] = None,
    ingestType: DisplayIngestType = CreateDisplayIngestType,
    space: DisplayStorageSpace = DisplayStorageSpace(randomAlphanumeric)
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
