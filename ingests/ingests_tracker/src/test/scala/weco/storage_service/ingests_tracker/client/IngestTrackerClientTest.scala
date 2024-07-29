package weco.storage_service.ingests_tracker.client

import java.time.Instant

import org.apache.pekko.http.scaladsl.model.Uri
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.ingests.models.{Ingest, IngestStatusUpdate}
import weco.storage_service.ingests.models.Ingest.Failed
import weco.storage_service.ingests.models.Ingest.Succeeded
import weco.storage_service.ingests_tracker.fixtures.IngestsTrackerApiFixture
import weco.storage_service.ingests_tracker.tracker.memory.MemoryIngestTracker

import scala.concurrent.ExecutionContext.Implicits.global

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
        val initialIngest = createIngestWith(status = Succeeded)
        val failedUpdate = createIngestStatusUpdateWith(
          id = initialIngest.id,
          status = Ingest.Failed
        )

        withIngestsTracker(initialIngest) { ingestsTracker =>
          withIngestTrackerClient(trackerUri) { client =>
            val update = client.updateIngest(failedUpdate)

            whenReady(update) { result =>
              result shouldBe Left(
                IngestTrackerUpdateConflictError(failedUpdate)
              )
              ingestsTracker
                .get(initialIngest.id)
                .right
                .get
                .identifiedT shouldBe initialIngest
            }
          }
        }
      }

      it("errors if you apply an update to a non-existent ingest") {
        val ingest = createIngest
        val update = createIngestEventUpdate

        withIngestsTracker(ingest) { _ =>
          withIngestTrackerClient(trackerUri) { client =>
            whenReady(client.updateIngest(update)) {
              _.left.value shouldBe IngestTrackerUpdateNonExistentIngestError(
                update
              )
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
              _.value shouldBe ingest
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

class PekkoIngestTrackerClientTest
    extends IngestTrackerClientTestCases
    with IntegrationPatience {
  override def withIngestTrackerClient[R](
    trackerUri: String
  )(testWith: TestWith[IngestTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      val client = new PekkoIngestTrackerClient(trackerHost = Uri(trackerUri))

      testWith(client)
    }
}
