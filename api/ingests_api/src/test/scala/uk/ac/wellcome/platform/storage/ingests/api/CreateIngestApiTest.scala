package uk.ac.wellcome.platform.storage.ingests.api

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser._
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.SourceLocationPayload
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.display._
import uk.ac.wellcome.platform.archive.display.ingests._
import uk.ac.wellcome.platform.storage.ingests.api.fixtures.IngestsApiFixture
import uk.ac.wellcome.storage.ObjectLocation

/** Tests for POST /ingests
  *
  */
class CreateIngestApiTest
    extends AnyFunSpec
    with Matchers
    with IngestsApiFixture
    with IntegrationPatience
    with JsonAssertions
    with StorageRandomThings {

  val contextUrlTest =
    "http://api.wellcomecollection.org/storage/v1/context.json"

  it("creates an ingest") {
    withConfiguredApp() {
      case (ingestTracker, messageSender, metrics, baseUrl) =>
        val url = s"$baseUrl/ingests"

        val bucketName = "bucket"
        val s3key = "key.txt"
        val spaceName = "somespace"

        val externalIdentifier = createExternalIdentifier

        val entity = createRequestWith(
          bucket = bucketName,
          key = s3key,
          space = spaceName,
          externalIdentifier = externalIdentifier
        )

        val expectedLocationR = s"$baseUrl/(.+)".r

        whenPostRequestReady(url, entity) { response: HttpResponse =>
          response.status shouldBe StatusCodes.Created

          val maybeId = response.headers.collectFirst {
            case HttpHeader("location", expectedLocationR(id)) => id
          }

          maybeId.isEmpty shouldBe false
          val id = UUID.fromString(maybeId.get)

          val ingestFuture =
            withMaterializer { implicit materializer =>
              Unmarshal(response.entity).to[ResponseDisplayIngest]
            }

          whenReady(ingestFuture) { actualIngest =>
            actualIngest.context shouldBe contextUrlTest
            actualIngest.id shouldBe id
            actualIngest.sourceLocation shouldBe DisplayLocation(
              provider = StandardDisplayProvider,
              bucket = bucketName,
              path = s3key
            )

            actualIngest.callback.isDefined shouldBe true
            actualIngest.callback.get.url shouldBe testCallbackUri.toString
            actualIngest.callback.get.status.get shouldBe DisplayStatus(
              "processing"
            )

            actualIngest.ingestType shouldBe CreateDisplayIngestType

            actualIngest.status shouldBe DisplayStatus("accepted")

            actualIngest.space shouldBe DisplayStorageSpace(spaceName)

            actualIngest.bag.info.externalIdentifier shouldBe externalIdentifier

            val expectedIngest = Ingest(
              id = IngestID(id),
              ingestType = CreateIngestType,
              sourceLocation = SourceLocation(
                provider = StandardStorageProvider,
                location = ObjectLocation(bucketName, s3key)
              ),
              space = StorageSpace(spaceName),
              callback = Some(Callback(testCallbackUri, Callback.Pending)),
              status = Ingest.Accepted,
              externalIdentifier = externalIdentifier,
              createdDate = Instant.parse(actualIngest.createdDate),
              events = Nil
            )

            assertIngestCreated(expectedIngest)(ingestTracker)

            val expectedPayload = SourceLocationPayload(expectedIngest)
            messageSender
              .getMessages[SourceLocationPayload] shouldBe Seq(
              expectedPayload
            )

            assertMetricSent(metrics, result = HttpMetricResults.Success)
          }
        }
    }
  }

  it("allows requesting an ingestType 'create'") {
    withConfiguredApp() {
      case (_, messageSender, metrics, baseUrl) =>
        val url = s"$baseUrl/ingests"

        val entity = createRequestWith(
          ingestType = "create"
        )

        whenPostRequestReady(url, entity) { response: HttpResponse =>
          response.status shouldBe StatusCodes.Created

          val actualIngest = getT[ResponseDisplayIngest](response.entity)
          actualIngest.ingestType.id shouldBe "create"

          val payload =
            messageSender.getMessages[SourceLocationPayload].head
          payload.context.ingestType shouldBe CreateIngestType

          assertMetricSent(metrics, result = HttpMetricResults.Success)
        }
    }
  }

  it("allows requesting an ingestType 'update'") {
    withConfiguredApp() {
      case (_, messageSender, metrics, baseUrl) =>
        val url = s"$baseUrl/ingests"

        val entity = createRequestWith(
          ingestType = "update"
        )

        whenPostRequestReady(url, entity) { response: HttpResponse =>
          response.status shouldBe StatusCodes.Created

          val actualIngest = getT[ResponseDisplayIngest](response.entity)

          actualIngest.ingestType.id shouldBe "update"

          val payload = messageSender.getMessages[SourceLocationPayload].head
          payload.context.ingestType shouldBe UpdateIngestType

          assertMetricSent(metrics, result = HttpMetricResults.Success)
        }
    }
  }

  it("creates an ingest with a slash in the external identifier") {
    withConfiguredApp() {
      case (_, _, _, baseUrl) =>
        val url = s"$baseUrl/ingests"

        val externalIdentifier = ExternalIdentifier("PP/MIA/1")

        val entity = createRequestWith(
          externalIdentifier = externalIdentifier
        )

        whenPostRequestReady(url, entity) { response: HttpResponse =>
          response.status shouldBe StatusCodes.Created

          val ingestFuture =
            withMaterializer { implicit materializer =>
              Unmarshal(response.entity).to[ResponseDisplayIngest]
            }

          whenReady(ingestFuture) {
            _.bag.info.externalIdentifier shouldBe externalIdentifier
          }
        }
    }
  }

  describe("returns a 400 error for malformed requests") {
    val json = createRequestJson

    describe("problems with the ingestType") {
      it("if the field is missing") {
        val badJson = root.obj.modify {
          _.remove("ingestType")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .ingestType: required property not supplied."
        )
      }

      it("if the id field is missing") {
        val badJson = root.ingestType.obj.modify {
          _.remove("id")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .ingestType.id: required property not supplied."
        )
      }

      it("if the field has an invalid value") {
        val badIngestType = randomAlphanumeric

        val badJson = root.ingestType.id.string.set(badIngestType)

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            s"""Invalid value at .ingestType.id: got "$badIngestType", valid values are: create, update."""
        )
      }
    }

    describe("problems with the space") {
      it("if the field is missing") {
        val badJson = root.obj.modify {
          _.remove("space")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .space: required property not supplied."
        )
      }

      it("if the id field is missing") {
        val badJson = root.space.obj.modify {
          _.remove("id")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .space.id: required property not supplied."
        )
      }

      it("if the space contains a slash") {
        val badJson = root.space.obj.modify {
          _.add("id", Json.fromString("alfa/bravo"))
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .space.id: must not contain slashes."
        )
      }

      it("if the space is empty") {
        val badJson = root.space.obj.modify {
          _.add("id", Json.fromString(""))
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage = "Invalid value at .space.id: must not be empty."
        )
      }
    }

    describe("problems with the bag") {
      it("if the field is missing") {
        val badJson = root.obj.modify {
          _.remove("bag")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .bag: required property not supplied."
        )
      }

      it("if the info field is missing") {
        val badJson = root.bag.obj.modify {
          _.remove("info")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .bag.info: required property not supplied."
        )
      }

      it("if the info.externalIdentifier field is missing") {
        val badJson = root.bag.info.obj.modify {
          _.remove("externalIdentifier")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .bag.info.externalIdentifier: required property not supplied."
        )
      }

      it("if the info.externalIdentifier field is empty") {
        val badJson = root.bag.info.obj.modify {
          _.add("externalIdentifier", Json.fromString(""))
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .bag.info.externalIdentifier: must not be empty."
        )
      }
    }

    describe("problems with the sourceLocation") {
      it("if the field is missing") {
        val badJson = root.obj.modify {
          _.remove("sourceLocation")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .sourceLocation: required property not supplied."
        )
      }

      it("if the provider field is missing") {
        val badJson = root.sourceLocation.obj.modify {
          _.remove("provider")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .sourceLocation.provider: required property not supplied."
        )
      }

      it("if the provider field has an invalid value") {
        val badJson = root.sourceLocation.provider.obj.modify {
          _.add("id", Json.fromString("not-a-storage-provider"))
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            """Invalid value at .sourceLocation.provider.id: got "not-a-storage-provider", valid values are: aws-s3-standard, aws-s3-ia."""
        )
      }

      it("if the bucket field is missing") {
        val badJson = root.sourceLocation.obj.modify {
          _.remove("bucket")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .sourceLocation.bucket: required property not supplied."
        )
      }

      it("if the path field is missing") {
        val badJson = root.sourceLocation.obj.modify {
          _.remove("path")
        }

        assertCatchesMalformedRequest(
          badJson(json).noSpaces,
          expectedMessage =
            "Invalid value at .sourceLocation.path: required property not supplied."
        )
      }
    }

    it("if the body is not valid JSON") {
      assertCatchesMalformedRequest(
        requestBody = "hgjh",
        expectedMessage =
          "The request content was malformed:\nexpected json value got 'hgjh' (line 1, column 1)"
      )
    }

    it("if the Content-Type is not an accepted type") {
      assertCatchesMalformedRequest(
        contentType = ContentTypes.`text/plain(UTF-8)`,
        expectedStatusCode = StatusCodes.UnsupportedMediaType,
        expectedMessage =
          "The request's Content-Type [Some(Some(text/plain; charset=UTF-8))] is not supported. Expected:\napplication/json",
        expectedLabel = "Unsupported Media Type"
      )
    }

    it("includes every error") {
      val badJson = root.obj.modify {
        _.remove("ingestType").remove("sourceLocation")
      }

      assertCatchesMalformedRequest(
        badJson(json).noSpaces,
        expectedMessage =
          """|Invalid value at .sourceLocation: required property not supplied.
             |Invalid value at .ingestType: required property not supplied.""".stripMargin
      )
    }
  }

  it("returns a 500 Server Error if updating the ingest starter fails") {
    withBrokenApp {
      case (_, _, metrics, baseUrl) =>
        whenPostRequestReady(s"$baseUrl/ingests/$randomUUID", createRequest) {
          response =>
            assertIsInternalServerErrorResponse(response)

            assertMetricSent(metrics, result = HttpMetricResults.ServerError)
        }
    }
  }

  def createRequestJsonWith(
    ingestType: String = CreateIngestType.id,
    bucket: String = randomAlphanumeric,
    key: String = randomAlphanumeric,
    space: String = randomAlphanumeric,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier
  ): Json =
    parse(
      s"""|{
          |  "type": "Ingest",
          |  "ingestType": {
          |    "id": "$ingestType",
          |    "type": "IngestType"
          |  },
          |  "sourceLocation":{
          |    "type": "Location",
          |    "provider": {
          |      "type": "Provider",
          |      "id": "${StandardDisplayProvider.id}"
          |    },
          |    "bucket": "$bucket",
          |    "path": "$key"
          |  },
          |  "space": {
          |    "id": "$space",
          |    "type": "Space"
          |  },
          |  "callback": {
          |    "url": "${testCallbackUri.toString}"
          |  },
          |  "bag": {
          |    "type": "Bag",
          |    "info": {
          |      "type": "BagInfo",
          |      "externalIdentifier": "${externalIdentifier.underlying}"
          |    }
          |  }
          |}""".stripMargin
    ).right.value

  def createRequestJson: Json =
    createRequestJsonWith()

  def createRequestWith(
    ingestType: String = CreateIngestType.id,
    bucket: String = randomAlphanumeric,
    key: String = randomAlphanumeric,
    space: String = randomAlphanumeric,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier
  ): RequestEntity =
    HttpEntity(
      ContentTypes.`application/json`,
      createRequestJsonWith(
        ingestType = ingestType,
        bucket = bucket,
        key = key,
        space = space,
        externalIdentifier = externalIdentifier
      ).noSpaces
    )

  def createRequest: RequestEntity =
    createRequestWith()

  private def assertCatchesMalformedRequest(
    requestBody: String = createRequestJson.noSpaces,
    contentType: ContentType.NonBinary = ContentTypes.`application/json`,
    expectedStatusCode: StatusCode = StatusCodes.BadRequest,
    expectedMessage: String,
    expectedLabel: String = "Bad Request"
  ) = {
    withConfiguredApp() {
      case (_, messageSender, metrics, baseUrl) =>
        val url = s"$baseUrl/ingests"

        val entity = HttpEntity(
          contentType = contentType,
          string = requestBody
        )

        whenPostRequestReady(url, entity) { response: HttpResponse =>
          assertIsUserErrorResponse(
            response = response,
            description = expectedMessage,
            statusCode = expectedStatusCode,
            label = expectedLabel
          )

          messageSender.messages shouldBe empty

          assertMetricSent(metrics, result = HttpMetricResults.UserError)
        }
    }
  }
}
