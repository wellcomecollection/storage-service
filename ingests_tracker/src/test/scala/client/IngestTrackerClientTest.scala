package client

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Failed
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{AkkaIngestTrackerClient, IngestTrackerConflictError, IngestTrackerUnknownError}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Succeeded
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture

class IngestTrackerClientTest
  extends AnyFunSpec
    with Matchers
    with Eventually
    with HttpFixtures
    with IngestsTrackerApiFixture
    with IngestGenerators
    with IntegrationPatience {

  implicit val as = ActorSystem()

  val host = "http://localhost:8080"

  val ingestTrackerClient = new AkkaIngestTrackerClient(Uri(host))

  val ingest = createIngestWith(
    createdDate = Instant.now()
  )

  val ingestStatusUpdate =
    createIngestStatusUpdateWith(
      id = ingest.id,
      status = Succeeded
    )

  val succeededIngest = ingest.copy(
    status = Succeeded,
    events = ingestStatusUpdate.events
  )

  val failedIngest = ingest.copy(
    status = Failed,
    events = ingestStatusUpdate.events
  )

  it("updates a valid ingest") {
    withIngestsTrackerApi(Seq(ingest)) {
      case (_, _, ingestsTracker) =>

        val update = ingestTrackerClient.updateIngest(ingestStatusUpdate)

        whenReady(update) { result =>
          result shouldBe Right(succeededIngest)
          ingestsTracker.get(ingest.id).right.get.identifiedT shouldBe succeededIngest
        }

    }
  }

  it("conflicts with a valid ingest") {
    withIngestsTrackerApi(Seq(failedIngest)) {
      case (_, _, ingestsTracker) =>

        val update = ingestTrackerClient.updateIngest(ingestStatusUpdate)

        whenReady(update) { result =>
          result shouldBe Left(IngestTrackerConflictError(ingestStatusUpdate))
          ingestsTracker.get(ingest.id).right.get.identifiedT shouldBe failedIngest
        }
    }
  }

  it("errors with a broken api") {
    withBrokenIngestsTrackerApi {
      case (_, _, _) =>
        val update = ingestTrackerClient.updateIngest(ingestStatusUpdate)

        whenReady(update) { result =>
          result shouldBe a[Left[_,_]]
          result.left.get shouldBe a[IngestTrackerUnknownError]
        }
    }
  }
}