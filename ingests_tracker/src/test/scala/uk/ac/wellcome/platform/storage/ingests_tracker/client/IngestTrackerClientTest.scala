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

  def withIngestTrackerClient[R](trackerUri: String)(
    testWith: TestWith[IngestTrackerClient, R]
  ): R

  private def withIngestsTracker[R](
    ingest: Ingest
  )(testWith: TestWith[MemoryIngestTracker, R]): R =
    withIngestsTrackerApi(Seq(ingest)) {
      case (_, _, ingestsTracker) =>
        testWith(ingestsTracker)
    }

  describe("behaves as an IngestTrackerClient") {
    describe("createIngest") {
      it("creates a new ingest") {
        withIngestsTrackerApi() {
          case (_, _, ingestsTracker) =>
            withIngestTrackerClient(trackerUri) { client =>
              val create = client.createIngest(ingest)

              whenReady(create) { result =>
                result shouldBe Right(())
                ingestsTracker
                  .get(ingest.id)
                  .right
                  .get
                  .identifiedT shouldBe ingest
              }
            }
        }
      }

      it("does not create a conflicting ingest") {
        withIngestsTracker(failedIngest) { ingestsTracker =>
          withIngestTrackerClient(trackerUri) { client =>
            val create = client.createIngest(ingest)

            whenReady(create) { result =>
              result shouldBe Left(IngestTrackerCreateConflictError(ingest))
              ingestsTracker
                .get(ingest.id)
                .right
                .get
                .identifiedT shouldBe failedIngest
            }
          }
        }
      }

      it("errors if the tracker API returns a 500 error") {
        withBrokenIngestsTrackerApi { _ =>
          withIngestTrackerClient(trackerUri) { client =>
            val future = client.createIngest(ingest)

            whenReady(future) {
              _.left.value shouldBe a[IngestTrackerUnknownCreateError]
            }
          }
        }
      }

      it("fails if the tracker API does not respond") {
        withIngestTrackerClient("http://localhost:9000/nothing") { client =>
          val future = client.createIngest(ingest)

          whenReady(future.failed) {
            _ shouldBe a[Throwable]
          }
        }
      }
    }

    describe("updateIngest") {
      it("applies a valid update") {
        withIngestsTracker(ingest) { ingestsTracker =>
          withIngestTrackerClient(trackerUri) { client =>
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
          withIngestTrackerClient(trackerUri) { client =>
            val update = client.updateIngest(ingestStatusUpdate)

            whenReady(update) { result =>
              result shouldBe Left(
                IngestTrackerUpdateConflictError(ingestStatusUpdate)
              )
              ingestsTracker
                .get(ingest.id)
                .right
                .get
                .identifiedT shouldBe failedIngest
            }
          }
        }
      }

      it("errors if the tracker API returns a 500 error") {
        withBrokenIngestsTrackerApi { _ =>
          withIngestTrackerClient(trackerUri) { client =>
            val update = client.updateIngest(ingestStatusUpdate)

            whenReady(update) {
              _.left.value shouldBe a[IngestTrackerUnknownUpdateError]
            }
          }
        }
      }

      it("fails if the tracker API does not respond") {
        withIngestTrackerClient("http://localhost:9000/nothing") { client =>
          val future = client.updateIngest(ingestStatusUpdate)

          whenReady(future.failed) {
            _ shouldBe a[Throwable]
          }
        }
      }
    }

    describe("getIngest") {
      it("finds an ingest") {
        withIngestsTracker(ingest) { _ =>
          withIngestTrackerClient(trackerUri) { client =>
            whenReady(client.getIngest(ingest.id)) {
              _.right.value shouldBe ingest
            }
          }
        }
      }

      it("returns a NotFoundError if you look up a non-existent ingest") {
        val nonExistentID = createIngestID

        withIngestsTracker(ingest) { _ =>
          withIngestTrackerClient(trackerUri) { client =>
            whenReady(client.getIngest(nonExistentID)) {
              _.left.value shouldBe IngestTrackerNotFoundError(nonExistentID)
            }
          }
        }
      }

      it("errors if the tracker API returns a 500 error") {
        withBrokenIngestsTrackerApi { _ =>
          withIngestTrackerClient(trackerUri) { client =>
            val future = client.getIngest(createIngestID)

            whenReady(future) {
              _.left.value shouldBe a[IngestTrackerUnknownGetError]
            }
          }
        }
      }

      it("fails if the tracker API does not respond") {
        withIngestTrackerClient("http://localhost:9000/nothing") { client =>
          val future = client.getIngest(createIngestID)

          whenReady(future.failed) {
            _ shouldBe a[Throwable]
          }
        }
      }
    }
  }
}

class AkkaIngestTrackerClientTest
    extends IngestTrackerClientTestCases
    with IntegrationPatience {
  override def withIngestTrackerClient[R](
    trackerUri: String
  )(testWith: TestWith[IngestTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      val client = new AkkaIngestTrackerClient(trackerHost = Uri(trackerUri))

      testWith(client)
    }
}
