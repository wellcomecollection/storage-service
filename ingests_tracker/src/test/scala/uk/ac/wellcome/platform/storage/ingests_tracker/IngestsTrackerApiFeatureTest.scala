package uk.ac.wellcome.platform.storage.ingests_tracker

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse, RequestEntity, StatusCodes}
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.fixtures.{HttpFixtures, StorageRandomThings}
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture
import uk.ac.wellcome.json.JsonUtil._
import io.circe.syntax._
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Succeeded

class IngestsTrackerApiFeatureTest
    extends FunSpec
    with Matchers
    with Akka
    with IngestsTrackerApiFixture
    with JsonAssertions
    with HttpFixtures
    with IntegrationPatience
    with EitherValues
    with StorageRandomThings {

  describe("GET /healthcheck") {
    it("responds OK") {
      withConfiguredApp() { _ =>
        val path = s"http://localhost:8080/healthcheck"

        whenGetRequestReady(path) { result =>
          result.status shouldBe StatusCodes.OK
        }
      }
    }
  }

  describe("POST /ingest") {
    val ingest = createIngestWith(
      createdDate = Instant.now(),
      version = None
    )

    it("responds with Created when successful") {
      withConfiguredApp() { ingestTracker =>
        val path = "http://localhost:8080/ingest"
        val entity = HttpEntity(
          ContentTypes.`application/json`,
          ingest.asJson.noSpaces
        )

        whenPostRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.Created

          val getIngest = ingestTracker.get(ingest.id)

          getIngest shouldBe a[Right[_,_]]
          getIngest.right.get.identifiedT shouldBe ingest
        }
      }
    }

    it("responds with Conflict when the ingest exists") {
      withConfiguredApp(Seq(ingest)) { _ =>
        val path = "http://localhost:8080/ingest"
        val entity = HttpEntity(
          ContentTypes.`application/json`,
          ingest.asJson.noSpaces
        )

        whenPostRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.Conflict
        }
      }
    }

    it("responds with InternalServerError when broken") {
      withBrokenApp { _ =>
        val path = "http://localhost:8080/ingest"
        val entity = HttpEntity(
          ContentTypes.`application/json`,
          ingest.asJson.noSpaces
        )

        whenPostRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.InternalServerError
        }
      }
    }
  }

  describe("PATCH /ingest/:id") {
    val ingest = createIngestWith(
      createdDate = Instant.now(),
      events = Seq(createIngestEvent, createIngestEvent).sortBy {
        _.createdDate
      },
      version = None
    )

    val ingestStatusUpdate =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Succeeded
      )

    // Cases:
    // - Updating existing ingest 200
    // - Updating existing ingest with bad json 400
    // - Updating existing ingest to incorrect state 409
    // - Updating non-existent ingest 409
    // - Broken app 500

    it("FAILS") {
      withConfiguredApp(Seq(ingest)) { _ =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        val entity = HttpEntity(
          ContentTypes.`application/json`,
          ingestStatusUpdate.asJson.noSpaces
        )

        whenPatchRequestReady(path, entity) { result =>
          result.status shouldBe StatusCodes.InternalServerError
        }
      }
    }
  }

  describe("GET /ingest/:id") {
    val ingest = createIngestWith(
      createdDate = Instant.now(),
      events = Seq(createIngestEvent, createIngestEvent).sortBy {
        _.createdDate
      },
      version = None
    )

    val notRecordedIngestId = createIngestID

    it("responds NotFound when there is no ingest") {
      withConfiguredApp(Seq(ingest)) { _ =>
        val path = s"http://localhost:8080/ingest/${notRecordedIngestId}"

        whenGetRequestReady(path) { response =>
          response.status shouldBe StatusCodes.NotFound
        }
      }
    }

    it("responds OK with an Ingest when available") {
      withConfiguredApp(Seq(ingest)) { _ =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        whenGetRequestReady(path) { response =>
          response.status shouldBe StatusCodes.OK

          // We are interested in whether we can serialise/de-serialise
          // what the JSON looks like is not interesting, so we
          // do not need to test in detail as with the external API
          withStringEntity(response.entity) { jsonString =>
            fromJson[Ingest](jsonString).get shouldBe ingest
          }
        }
      }
    }

    it("responds InternalServerError when broken") {
      withBrokenApp { _ =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        whenGetRequestReady(path) { response =>
          response.status shouldBe StatusCodes.InternalServerError
        }
      }
    }
  }

  // Required as not yet provided by HTTP Fixtures
  def whenPatchRequestReady[R](url: String, entity: RequestEntity)(
    testWith: TestWith[HttpResponse, R]
  ): R = {
    withActorSystem { implicit actorSystem =>

      val r = HttpRequest(
        method = PATCH,
        uri = url,
        headers = Nil,
        entity = entity
      )

      val request = Http().singleRequest(r)

      whenReady(request) { response: HttpResponse =>
        testWith(response)
      }
    }
  }
}
