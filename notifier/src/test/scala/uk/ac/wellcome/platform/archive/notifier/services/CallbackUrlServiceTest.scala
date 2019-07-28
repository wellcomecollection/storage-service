package uk.ac.wellcome.platform.archive.notifier.services

import java.net.URI
import java.time.Instant

import akka.http.scaladsl.model.{ContentTypes, HttpMethods, HttpRequest, StatusCodes}
import akka.stream.StreamTcpException
import akka.stream.scaladsl.Sink
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.notifier.fixtures.{LocalWireMockFixture, NotifierFixtures}

class CallbackUrlServiceTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with NotifierFixtures
    with LocalWireMockFixture
    with IngestGenerators
    with EitherValues
    with JsonAssertions {

  describe("sends the HTTP requests") {
    it("returns a Success if the request succeeds") {
      withActorSystem { implicit actorSystem =>
        withCallbackUrlService { service =>
          val ingest = createIngest

          val future = service.getHttpResponse(
            ingest = ingest,
            callbackUri =
              new URI(s"http://$callbackHost:$callbackPort/callback/${ingest.id}")
          )

          whenReady(future) { result =>
            result.status shouldBe StatusCodes.NotFound
          }
        }
      }
    }

    it("returns a failed future if the HTTP request fails") {
      withActorSystem { implicit actorSystem =>
        withCallbackUrlService { service =>
          val ingest = createIngest

          val future = service.getHttpResponse(
            ingest = ingest,
            callbackUri = new URI(s"http://nope.nope/callback/${ingest.id}")
          )

          whenReady(future.failed) { result =>
            result shouldBe a[StreamTcpException]
          }
        }
      }
    }
  }

  describe("builds the correct HTTP request") {
    it("creates a JSON string") {
      val ingestId = createIngestID

      val callbackUri = new URI(s"http://example.org/callback/$ingestId")

      val ingest = createIngestWith(
        id = ingestId,
        callback = Some(createCallbackWith(uri = callbackUri)),
        version = None,
        events = Seq(createIngestEvent, createIngestEvent),
        lastModifiedDate = Some(Instant.now())
      )

      val request = buildRequest(ingest, callbackUri)

      assertIsJsonRequest(request, uri = callbackUri) { requestJsonString =>
        assertJsonStringsAreEqual(
          requestJsonString,
          s"""
             |{
             |  "@context": "http://localhost/context.json",
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
             |      "id": "aws-s3-standard"
             |    },
             |    "bucket": "${ingest.sourceLocation.location.namespace}",
             |    "path": "${ingest.sourceLocation.location.path}"
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

    it("omits null values from the JSON") {
      val callbackUri = new URI(s"http://example.org/callback/$createIngestID")

      val ingest = createIngestWith(
        lastModifiedDate = None
      )

      val request = buildRequest(ingest, callbackUri)

      assertIsJsonRequest(request, uri = callbackUri) { requestJsonString =>
        val json = parse(requestJsonString).right.value
        root.lastModifiedDate.string.getOption(json) shouldBe None
      }
    }

    it("includes the version, if present") {
      val callbackUri = new URI(s"http://example.org/callback/$createIngestID")

      val ingest = createIngestWith(
        version = Some(BagVersion(3))
      )

      val request = buildRequest(ingest, callbackUri)

      assertIsJsonRequest(request, uri = callbackUri) { requestJsonString =>
        val json = parse(requestJsonString).right.value
        root.bag.info.version.string.getOption(json) shouldBe Some("v3")
      }
    }

    def buildRequest(ingest: Ingest, callbackUri: URI): HttpRequest =
      withCallbackUrlService { service =>
        service.buildHttpRequest(
          ingest = ingest,
          callbackUri = callbackUri
        )
      }

    def assertIsJsonRequest(request: HttpRequest, uri: URI)(assertJson: String => Assertion): Assertion = {
      request.method shouldBe HttpMethods.POST
      request.uri.toString() shouldBe uri.toString

      withMaterializer { implicit materializer =>
        val future = request.entity.dataBytes.runWith(Sink.seq)
        whenReady(future) { byteString =>
          assertJson(byteString.head.utf8String)
        }
      }

      request.entity.contentType shouldBe ContentTypes.`application/json`
    }
  }
}
