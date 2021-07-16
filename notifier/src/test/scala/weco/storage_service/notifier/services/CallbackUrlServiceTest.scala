package weco.storage_service.notifier.services

import java.net.URI
import akka.http.scaladsl.model._
import io.circe.parser.parse
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{Assertion, EitherValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.http.client.{HttpClient, MemoryHttpClient}
import weco.http.fixtures.HttpFixtures
import weco.json.JsonUtil._
import weco.json.utils.JsonAssertions
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.display.ingests.ResponseDisplayIngest
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models.S3SourceLocation

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CallbackUrlServiceTest
    extends AnyFunSpec
    with Matchers
    with ScalaFutures
    with EitherValues
    with IntegrationPatience
    with IngestGenerators
    with HttpFixtures
    with JsonAssertions {

  describe("sends the request successfully") {
    it("returns a Success if the request succeeds") {
      val ingest = createIngest
      val callbackUri = s"http://example.org/callback/${ingest.id}"

      val client = new HttpClient {
        override def singleRequest(request: HttpRequest): Future[HttpResponse] =
          Future.successful(
            HttpResponse(status = StatusCodes.NotFound)
          )
      }

      val callbackUrlService = new CallbackUrlService(client = client)

      val future = callbackUrlService.getHttpResponse(
        ingest = ingest,
        callbackUri = new URI(callbackUri)
      )

      whenReady(future) { result =>
        result.isSuccess shouldBe true
        result.get.status shouldBe StatusCodes.NotFound
      }
    }

    it("returns a failed future if the HTTP request fails") {
      val exception = new Throwable("BOOM!")

      val client = new HttpClient {
        override def singleRequest(request: HttpRequest): Future[HttpResponse] =
          Future.failed(exception)
      }

      val callbackUrlService = new CallbackUrlService(client = client)

      val ingest = createIngest

      val future = callbackUrlService.getHttpResponse(
        ingest = ingest,
        callbackUri = new URI(s"http://nope.nope/callback/${ingest.id}")
      )

      whenReady(future) { result =>
        result.isFailure shouldBe true
        result.failed.get shouldBe exception
      }
    }
  }

  describe("builds the correct HTTP request") {
    val service = new CallbackUrlService(client = new MemoryHttpClient(responses = Seq()))

    it("creates a JSON string") {
      val ingestId = createIngestID

      val callbackUri = new URI(s"http://example.org/callback/$ingestId")

      val ingest = createIngestWith(
        id = ingestId,
        callback = Some(createCallbackWith(uri = callbackUri)),
        events = createIngestEvents(count = 2),
        version = None
      )

      val request = service.buildHttpRequest(ingest = ingest, callbackUri = callbackUri)

      val ingestLocation =
        ingest.sourceLocation.asInstanceOf[S3SourceLocation]

      assertIsJsonRequest(request, uri = callbackUri) { requestJsonString =>
        assertJsonStringsAreEqual(
          requestJsonString,
          s"""
             |{
             |  "id": "${ingestId.toString}",
             |  "type": "Ingest",
             |  "ingestType": {
             |    "id": "${ingest.ingestType.id}",
             |    "type": "IngestType"
             |  },
             |  "space": {
             |    "id": "${ingest.space.underlying}",
             |    "type": "Space"
             |  },
             |  "bag": {
             |    "type": "Bag",
             |    "info": {
             |      "type": "BagInfo",
             |      "externalIdentifier": "${ingest.externalIdentifier.underlying}"
             |    }
             |  },
             |  "status": {
             |    "id": "${ingest.status.toString}",
             |    "type": "Status"
             |  },
             |  "sourceLocation": {
             |    "type": "Location",
             |    "provider": {
             |      "type": "Provider",
             |      "id": "amazon-s3"
             |    },
             |    "bucket": "${ingestLocation.location.bucket}",
             |    "path": "${ingestLocation.location.key}"
             |  },
             |  "callback": {
             |    "type": "Callback",
             |    "url": "${ingest.callback.get.uri}",
             |    "status": {
             |      "id": "${ingest.callback.get.status.toString}",
             |      "type": "Status"
             |    }
             |  },
             |  "createdDate": "${ingest.createdDate}",
             |  "lastModifiedDate": "${ingest.lastModifiedDate.get}",
             |  "events": [
             |    {
             |      "type": "IngestEvent",
             |      "createdDate": "${ingest.events(0).createdDate}",
             |      "description": "${ingest.events(0).description}"
             |    },
             |    {
             |      "type": "IngestEvent",
             |      "createdDate": "${ingest.events(1).createdDate}",
             |      "description": "${ingest.events(1).description}"
             |    }
             |  ]
             |}
                 """.stripMargin
        )
      }
    }

    it("includes the version, if present") {
      val callbackUri = new URI(s"http://example.org/callback/$createIngestID")

      val ingest = createIngestWith(
        version = Some(BagVersion(3))
      )

      val request = service.buildHttpRequest(ingest, callbackUri)

      assertIsJsonRequest(request, uri = callbackUri) { requestJsonString =>
        val ingest = fromJson[ResponseDisplayIngest](requestJsonString).get
        ingest.bag.info.version shouldBe Some("v3")
      }
    }

    it("omits null values from the JSON") {
      val callbackUri = new URI(s"http://example.org/callback/$createIngestID")

      val ingest = createIngestWith(
        events = Seq.empty
      )

      ingest.lastModifiedDate shouldBe None

      val request = service.buildHttpRequest(ingest, callbackUri)

      assertIsJsonRequest(request, uri = callbackUri) { requestJsonString =>
        val json = parse(requestJsonString).value
        json.asObject.get.keys should not contain("lastModifiedDate")
      }
    }

    def assertIsJsonRequest(request: HttpRequest, uri: URI)(
      assertJson: String => Assertion
    ): Assertion = {
      request.method shouldBe HttpMethods.POST
      request.uri.toString() shouldBe uri.toString

      withStringEntity(request.entity) {
        assertJson(_)
      }

      request.entity.contentType shouldBe ContentTypes.`application/json`
    }
  }
}
