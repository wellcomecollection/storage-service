package weco.storage_service.ingests_worker.services

import akka.http.scaladsl.model.Uri
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.messaging.worker.models.{
  Result,
  RetryableFailure,
  Successful,
  TerminalFailure
}
import weco.storage_service.ingests.models.Ingest
import weco.storage_service.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import weco.storage_service.ingests_tracker.fixtures.IngestsTrackerApiFixture
import weco.storage_service.ingests_worker.fixtures.IngestsWorkerFixtures
import weco.http.fixtures.HttpFixtures

import scala.concurrent.ExecutionContext.Implicits.global

class IngestsWorkerServiceTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with HttpFixtures
    with ScalaFutures
    with IngestsWorkerFixtures
    with IngestsTrackerApiFixture
    with IntegrationPatience {

  describe("When the client succeeds") {
    it("returns Successful") {
      withIngestWorker(ingestTrackerClient = successfulClient(ingest)) {
        worker =>
          whenReady(worker.processMessage(ingestStatusUpdate)) {
            result: Result[Ingest] =>
              result shouldBe a[Successful[_]]
          }
      }
    }
  }

  describe("When the client return a conflict") {
    it("returns a terminal failure") {
      withIngestWorker(ingestTrackerClient = conflictClient(ingestStatusUpdate)) {
        worker =>
          whenReady(worker.processMessage(ingestStatusUpdate)) {
            _ shouldBe a[TerminalFailure[_]]
          }
      }
    }
  }

  describe("When the client return an unknown error") {
    it("returns a retryable failure") {
      withIngestWorker(
        ingestTrackerClient = unknownErrorClient(ingestStatusUpdate)
      ) { worker =>
        whenReady(worker.processMessage(ingestStatusUpdate)) {
          _ shouldBe a[RetryableFailure[_]]
        }
      }
    }
  }

  describe("When the client fails") {
    it("returns a retryable failure") {
      withIngestWorker(ingestTrackerClient = failedFutureClient()) { worker =>
        whenReady(worker.processMessage(ingestStatusUpdate)) {
          _ shouldBe a[RetryableFailure[_]]
        }
      }
    }
  }

  it("updating a non-existent ingest is a non-deterministic failure") {
    withIngestsTrackerApi(initialIngests = Seq.empty) { _ =>
      withIngestTrackerClient(trackerUri) { client =>
        withIngestWorker(ingestTrackerClient = client) { worker =>
          whenReady(worker.processMessage(createIngestEventUpdate)) {
            _ shouldBe a[RetryableFailure[_]]
          }
        }
      }
    }
  }

  def withIngestTrackerClient[R](
    trackerUri: String
  )(testWith: TestWith[IngestTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      val client = new AkkaIngestTrackerClient(trackerHost = Uri(trackerUri))

      testWith(client)
    }
}
