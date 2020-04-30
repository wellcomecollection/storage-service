package uk.ac.wellcome.platform.storage.ingests_tracker

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import io.circe.syntax._
import org.scalatest.EitherValues
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.fixtures.{
  HttpFixtures,
  StorageRandomThings
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.{
  Failed,
  Succeeded
}
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture

class IngestsTrackerApiFeatureTest
    extends AnyFunSpec
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

    it("responds Created when successful") {
      withConfiguredApp() { ingestTracker =>
        val path = "http://localhost:8080/ingest"
        val entity = HttpEntity(
          ContentTypes.`application/json`,
          ingest.asJson.noSpaces
        )

        whenPostRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.Created

          val getIngest = ingestTracker.get(ingest.id)

          getIngest shouldBe a[Right[_, _]]
          getIngest.right.get.identifiedT shouldBe ingest
        }
      }
    }

    it("responds Conflict when the ingest exists") {
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

    it("responds BadRequest when bad json is sent") {
      withConfiguredApp(Seq(ingest)) { _ =>
        val path = "http://localhost:8080/ingest"
        val entity = HttpEntity(
          ContentTypes.`application/json`,
          randomAlphanumeric
        )

        whenPostRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.BadRequest
        }
      }
    }

    it("responds InternalServerError when broken") {
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
      events = Seq(createIngestEvent, createIngestEvent),
      version = None
    )

    val ingestEvent = createIngestEventUpdateWith(id = ingest.id)

    val ingestStatusUpdateSucceeded =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Succeeded
      )

    val ingestStatusUpdateFailed =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Failed
      )

    it("responds OK when updating with a valid IngestEventUpdate") {
      withConfiguredApp(Seq(ingest)) { ingestTracker =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        val entity = HttpEntity(
          ContentTypes.`application/json`,
          toJson[IngestUpdate](ingestEvent).get
        )

        whenPatchRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.OK

          withStringEntity(response.entity) { jsonString =>
            val updatedIngestResponse = fromJson[Ingest](jsonString).get
            updatedIngestResponse.events should contain allElementsOf (ingestEvent.events)
          }

          val ingestFromTracker =
            ingestTracker.get(ingest.id).right.get.identifiedT
          ingestFromTracker.events should contain allElementsOf (ingestEvent.events)
        }
      }
    }

    it("responds OK when updating with a valid IngestStatusUpdate") {
      withConfiguredApp(Seq(ingest)) { ingestTracker =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        val entity = HttpEntity(
          ContentTypes.`application/json`,
          toJson[IngestUpdate](ingestStatusUpdateSucceeded).get
        )

        whenPatchRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.OK

          withStringEntity(response.entity) { jsonString =>
            val updatedIngestResponse = fromJson[Ingest](jsonString).get
            updatedIngestResponse.status shouldBe ingestStatusUpdateSucceeded.status
            updatedIngestResponse.events should contain allElementsOf (ingestStatusUpdateSucceeded.events)
          }

          val ingestFromTracker =
            ingestTracker.get(ingest.id).right.get.identifiedT
          ingestFromTracker.status shouldBe ingestStatusUpdateSucceeded.status
          ingestFromTracker.events should contain allElementsOf (ingestStatusUpdateSucceeded.events)
        }
      }
    }

    it("responds Conflict when updating to an invalid Status") {
      withConfiguredApp(Seq(ingest)) { _ =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        val updateSucceededEntity = HttpEntity(
          ContentTypes.`application/json`,
          toJson[IngestUpdate](ingestStatusUpdateSucceeded).get
        )

        val updateFailedEntity = HttpEntity(
          ContentTypes.`application/json`,
          toJson[IngestUpdate](ingestStatusUpdateFailed).get
        )

        whenPatchRequestReady(path, updateSucceededEntity) { response =>
          response.status shouldBe StatusCodes.OK
          whenPatchRequestReady(path, updateFailedEntity) { response =>
            response.status shouldBe StatusCodes.Conflict
          }
        }
      }
    }

    it("responds Conflict when updating a non existent ingest") {
      val succeededIngest = ingest.copy(status = Succeeded)

      withConfiguredApp() { _ =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        val entity = HttpEntity(
          ContentTypes.`application/json`,
          toJson[IngestUpdate](ingestEvent).get
        )

        whenPatchRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.Conflict
        }
      }
    }

    it("responds InternalServerError when broken") {
      withBrokenApp { _ =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        val entity = HttpEntity(
          ContentTypes.`application/json`,
          toJson[IngestUpdate](ingestEvent).get
        )

        whenPatchRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.InternalServerError
        }
      }
    }

    it("responds BadRequest when updating with invalid json") {
      withConfiguredApp(Seq(ingest)) { _ =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        val entity = HttpEntity(
          ContentTypes.`application/json`,
          randomAlphanumeric
        )

        whenPatchRequestReady(path, entity) { response =>
          response.status shouldBe StatusCodes.BadRequest
        }
      }
    }
  }

  describe("GET /ingest/:id") {
    val ingest = createIngestWith(
      createdDate = Instant.now(),
      events = Seq(createIngestEvent, createIngestEvent),
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
