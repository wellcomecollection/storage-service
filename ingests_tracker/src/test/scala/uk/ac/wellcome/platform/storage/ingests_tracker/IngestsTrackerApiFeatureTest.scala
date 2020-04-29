package uk.ac.wellcome.platform.storage.ingests_tracker

import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.fixtures.{
  HttpFixtures,
  StorageRandomThings
}
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest

class IngestsTrackerApiFeatureTest
    extends FunSpec
    with Matchers
    with IngestsTrackerApiFixture
    with JsonAssertions
    with HttpFixtures
    with IntegrationPatience
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

        whenGetRequestReady(path) { result =>
          result.status shouldBe StatusCodes.NotFound
        }
      }
    }

    it("responds OK with an Ingest when available") {
      withConfiguredApp(Seq(ingest)) { _ =>
        val path = s"http://localhost:8080/ingest/${ingest.id}"

        whenGetRequestReady(path) { result =>
          result.status shouldBe StatusCodes.OK

          // We are interested in whether we can serialise/de-serialise
          // what the JSON looks like is not interesting, so we
          // do not need to test in detail as with the external API
          withStringEntity(result.entity) { jsonString =>
            fromJson[Ingest](jsonString).get shouldBe ingest
          }
        }
      }
    }
  }
}
