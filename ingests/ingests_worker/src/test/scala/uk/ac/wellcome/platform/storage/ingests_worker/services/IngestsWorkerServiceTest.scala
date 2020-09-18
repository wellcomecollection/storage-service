package uk.ac.wellcome.platform.storage.ingests_worker.services

import akka.http.scaladsl.model.Uri
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.worker.models.{
  DeterministicFailure,
  NonDeterministicFailure,
  Result,
  Successful
}
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  AkkaIngestTrackerClient,
  IngestTrackerClient
}
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture
import uk.ac.wellcome.platform.storage.ingests_worker.fixtures.IngestsWorkerFixtures

class IngestsWorkerServiceTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with HttpFixtures
    with ScalaFutures
    with IngestsWorkerFixtures
    with IngestsTrackerApiFixture
    with IntegrationPatience {

  val visibilityTimeout = 5

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
    it("returns DeterministicFailure") {
      withIngestWorker(ingestTrackerClient = conflictClient(ingestStatusUpdate)) {
        worker =>
          whenReady(worker.processMessage(ingestStatusUpdate)) {
            result: Result[Ingest] =>
              result shouldBe a[DeterministicFailure[_]]
          }
      }
    }
  }

  describe("When the client return an unknown error") {
    it("returns NonDeterministicFailure") {
      withIngestWorker(
        ingestTrackerClient = unknownErrorClient(ingestStatusUpdate)
      ) { worker =>
        whenReady(worker.processMessage(ingestStatusUpdate)) {
          result: Result[Ingest] =>
            result shouldBe a[NonDeterministicFailure[_]]
        }
      }
    }
  }

  describe("When the client fails") {
    it("returns NonDeterministicFailure") {
      withIngestWorker(ingestTrackerClient = failedFutureClient()) { worker =>
        whenReady(worker.processMessage(ingestStatusUpdate)) {
          result: Result[Ingest] =>
            result shouldBe a[NonDeterministicFailure[_]]
        }
      }
    }
  }

  it("updating a non-existent ingest is a non-deterministic failure") {
    withIngestsTrackerApi(initialIngests = Seq.empty) { _ =>
      withIngestTrackerClient(trackerUri) { client =>
        withIngestWorker(ingestTrackerClient = client) { worker =>
          whenReady(worker.processMessage(createIngestEventUpdate)) {
            _ shouldBe a[NonDeterministicFailure[_]]
          }
        }
      }
    }
  }

  def withIngestTrackerClient[R](trackerUri: String)(testWith: TestWith[IngestTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      val client = new AkkaIngestTrackerClient(trackerHost = Uri(trackerUri))

      testWith(client)
    }

}
