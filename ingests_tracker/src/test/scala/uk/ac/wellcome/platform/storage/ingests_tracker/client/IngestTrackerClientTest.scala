package uk.ac.wellcome.platform.storage.ingests_tracker.client

import java.time.Instant

import akka.http.scaladsl.model.Uri
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Failed
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Succeeded
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture

trait IngestTrackerClientTestCases
  extends AnyFunSpec
    with Matchers
    with IngestsTrackerApiFixture
    with ScalaFutures {

  val ingest: Ingest = createIngestWith(
    createdDate = Instant.now()
  )

  val ingestStatusUpdate: IngestStatusUpdate =
    createIngestStatusUpdateWith(
      id = ingest.id,
      status = Succeeded
    )

  val succeededIngest: Ingest = ingest.copy(
    status = Succeeded,
    events = ingestStatusUpdate.events
  )

  val failedIngest: Ingest = ingest.copy(
    status = Failed,
    events = ingestStatusUpdate.events
  )

  def withIngestTrackerClient[R](testWith: TestWith[IngestTrackerClient, R]): R

  private def withIngestsTracker[R](ingest: Ingest)(testWith: TestWith[MemoryIngestTracker, R]): R =
    withIngestsTrackerApi(Seq(ingest)) {
      case (_, _, ingestsTracker) =>
        testWith(ingestsTracker)
    }

  describe("behaves as an IngestTrackerClient") {
    describe("updateIngest") {
      it("applies a valid update") {
        withIngestsTracker(ingest) { ingestsTracker =>
          withIngestTrackerClient { client =>
            val update = client.updateIngest(ingestStatusUpdate)

            whenReady(update) { result =>
              result shouldBe Right(succeededIngest)
              ingestsTracker
                .get(ingest.id)
                .right
                .get
                .identifiedT shouldBe succeededIngest
            }
          }
        }
      }

      it("fails to apply a conflicting update") {
        withIngestsTracker(failedIngest) { ingestsTracker =>
          withIngestTrackerClient { client =>
            val update = client.updateIngest(ingestStatusUpdate)

            whenReady(update) { result =>
              result shouldBe Left(IngestTrackerConflictError(ingestStatusUpdate))
              ingestsTracker
                .get(ingest.id)
                .right
                .get
                .identifiedT shouldBe failedIngest
            }
          }
        }
      }

      it("errors if the tracker API is down") {
        withBrokenIngestsTrackerApi { _ =>
          withIngestTrackerClient { client =>
            val update = client.updateIngest(ingestStatusUpdate)

            whenReady(update) { result =>
              result shouldBe a[Left[_, _]]
              result.left.get shouldBe a[IngestTrackerUnknownError]
            }
          }
        }
      }
    }
  }
}

class AkkaIngestTrackerClientTest extends IngestTrackerClientTestCases with IntegrationPatience {
  override def withIngestTrackerClient[R](testWith: TestWith[IngestTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      val client = new AkkaIngestTrackerClient(trackerHost = Uri("http://localhost:8080"))

      testWith(client)
    }
}
