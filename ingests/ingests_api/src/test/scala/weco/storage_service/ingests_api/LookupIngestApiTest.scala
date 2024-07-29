package weco.storage_service.ingests_api

import java.time.Instant

import org.apache.pekko.http.scaladsl.model._
import io.circe.optics.JsonPath.root
import io.circe.parser._
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.utils.JsonAssertions
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.ingests.models.S3SourceLocation
import weco.storage_service.ingests_api.fixtures.IngestsApiFixture
import weco.http.monitoring.HttpMetricResults

/** Tests for GET /ingests/:id
  *
  */
class LookupIngestApiTest
    extends AnyFunSpec
    with Matchers
    with IngestsApiFixture
    with IntegrationPatience
    with JsonAssertions {

  it("finds an ingest") {
    val ingest = createIngestWith(
      createdDate = Instant.now(),
      events = Seq(createIngestEvent, createIngestEvent).sortBy {
        _.createdDate
      },
      version = None
    )

    withConfiguredApp(initialIngests = Seq(ingest)) {
      case (_, _, metrics, baseUrl) =>
        whenGetRequestReady(s"/ingests/${ingest.id}") { result =>
          result.status shouldBe StatusCodes.OK

          val sourceLocation =
            ingest.sourceLocation.asInstanceOf[S3SourceLocation]

          withStringEntity(result.entity) { jsonString =>
            assertJsonStringsAreEqual(
              jsonString,
              s"""
                 |{
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
                 |      "id": "amazon-s3"
                 |    },
                 |    "bucket": "${sourceLocation.location.bucket}",
                 |    "path": "${sourceLocation.location.key}"
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

          assertMetricSent(
            metricsName,
            metrics,
            result = HttpMetricResults.Success
          )
        }
    }
  }

  it("includes the version, if available") {
    val ingest = createIngestWith(
      version = Some(BagVersion(3))
    )

    withConfiguredApp(initialIngests = Seq(ingest)) {
      case (_, _, _, baseUrl) =>
        whenGetRequestReady(s"/ingests/${ingest.id}") { result =>
          withStringEntity(result.entity) { jsonString =>
            val json = parse(jsonString).value
            root.bag.info.version.string.getOption(json) shouldBe Some("v3")
          }
        }
    }
  }

  it("does not output empty values") {
    val ingest = createIngestWith(callback = None)

    withConfiguredApp(initialIngests = Seq(ingest)) {
      case (_, _, metrics, baseUrl) =>
        whenGetRequestReady(s"/ingests/${ingest.id}") { result =>
          result.status shouldBe StatusCodes.OK
          withStringEntity(result.entity) { jsonString =>
            val infoJson = parse(jsonString).right.get
            infoJson.findAllByKey("callback") shouldBe empty
          }

          assertMetricSent(
            metricsName,
            metrics,
            result = HttpMetricResults.Success
          )
        }
    }
  }

  it("returns a 404 Not Found if there is no matching ingest") {
    withConfiguredApp() {
      case (_, _, metrics, baseUrl) =>
        val id = randomUUID
        whenGetRequestReady(s"/ingests/$id") { response =>
          assertIsDisplayError(
            response,
            description = s"Ingest $id not found",
            statusCode = StatusCodes.NotFound
          )

          assertMetricSent(
            metricsName,
            metrics,
            result = HttpMetricResults.UserError
          )
        }
    }
  }

  it("returns a 500 Server Error if looking up the ingest fails") {
    withBrokenApp {
      case (_, _, metrics, baseUrl) =>
        whenGetRequestReady(s"/ingests/$createIngestID") { response =>
          assertIsDisplayError(
            response,
            statusCode = StatusCodes.InternalServerError
          )

          assertMetricSent(
            metricsName,
            metrics,
            result = HttpMetricResults.ServerError
          )
        }
    }
  }
}
