package uk.ac.wellcome.platform.storage.ingests.api

import java.time.Instant

import akka.http.scaladsl.model._
import io.circe.optics.JsonPath.root
import io.circe.parser._
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.storage.ingests.api.fixtures.IngestsApiFixture

/** Tests for GET /ingests/:id
  *
  */
class LookupIngestApiTest
    extends AnyFunSpec
    with Matchers
    with IngestsApiFixture
    with IntegrationPatience
    with JsonAssertions {

  val contextUrlTest =
    "http://api.wellcomecollection.org/storage/v1/context.json"

  it("returns a ingest tracker when available") {
    val ingest = createIngestWith(
      createdDate = Instant.now(),
      events = Seq(createIngestEvent, createIngestEvent).sortBy {
        _.createdDate
      },
      version = None
    )

    withConfiguredApp(initialIngests = Seq(ingest)) {
      case (_, _, metrics, baseUrl) =>
        whenGetRequestReady(s"$baseUrl/ingests/${ingest.id}") { result =>
          result.status shouldBe StatusCodes.OK

          withStringEntity(result.entity) { jsonString =>
            assertJsonStringsAreEqual(
              jsonString,
              s"""
                 |{
                 |  "@context": "http://api.wellcomecollection.org/storage/v1/context.json",
                 |  "id": "${ingest.id.toString}",
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
                 |""".stripMargin
            )
          }

          assertMetricSent(metrics, result = HttpMetricResults.Success)
        }
    }
  }

  it("includes the version, if available") {
    val ingest = createIngestWith(
      version = Some(BagVersion(3))
    )

    withConfiguredApp(initialIngests = Seq(ingest)) {
      case (_, _, _, baseUrl) =>
        whenGetRequestReady(s"$baseUrl/ingests/${ingest.id}") { result =>
          withStringEntity(result.entity) { jsonString =>
            val json = parse(jsonString).right.value
            root.bag.info.version.string.getOption(json) shouldBe Some("v3")
          }
        }
    }
  }

  it("does not output empty values") {
    val ingest = createIngestWith(callback = None)

    withConfiguredApp(initialIngests = Seq(ingest)) {
      case (_, _, metrics, baseUrl) =>
        whenGetRequestReady(s"$baseUrl/ingests/${ingest.id}") { result =>
          result.status shouldBe StatusCodes.OK
          withStringEntity(result.entity) { jsonString =>
            val infoJson = parse(jsonString).right.get
            infoJson.findAllByKey("callback") shouldBe empty
          }

          assertMetricSent(metrics, result = HttpMetricResults.Success)
        }
    }
  }

  it("returns a 404 NotFound if no ingest tracker matches id") {
    withConfiguredApp() {
      case (_, _, metrics, baseUrl) =>
        val id = randomUUID
        whenGetRequestReady(s"$baseUrl/ingests/$id") { response =>
          assertIsUserErrorResponse(
            response,
            description = s"Ingest $id not found",
            statusCode = StatusCodes.NotFound,
            label = "Not Found"
          )

          assertMetricSent(metrics, result = HttpMetricResults.UserError)
        }
    }
  }

  it("returns a 500 Server Error if reading from DynamoDB fails") {
    withBrokenApp {
      case (_, _, metrics, baseUrl) =>
        whenGetRequestReady(s"$baseUrl/ingests/$randomUUID") { response =>
          assertIsInternalServerErrorResponse(response)

          assertMetricSent(metrics, result = HttpMetricResults.ServerError)
        }
    }
  }
}
