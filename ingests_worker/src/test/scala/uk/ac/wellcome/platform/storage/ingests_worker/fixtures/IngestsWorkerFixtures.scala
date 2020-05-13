package uk.ac.wellcome.platform.storage.ingests_worker.fixtures

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Succeeded
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests_tracker.client.{
  IngestTrackerClient,
  IngestTrackerConflictError,
  IngestTrackerError,
  IngestTrackerUnknownError
}
import uk.ac.wellcome.platform.storage.ingests_worker.services.IngestsWorkerService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait IngestsWorkerFixtures
    extends ScalaFutures
    with Akka
    with IngestGenerators
    with AlpakkaSQSWorkerFixtures
    with IngestTrackerFixtures {

  val ingest = createIngestWith(
    createdDate = Instant.now()
  )

  val ingestStatusUpdate =
    createIngestStatusUpdateWith(
      id = ingest.id,
      status = Succeeded
    )

  val error = new Exception("BOOM!")

  class FakeIngestTrackerClient(
    response: Future[Either[IngestTrackerError, Ingest]]
  ) extends IngestTrackerClient {
    override def updateIngest(
      ingestUpdate: IngestUpdate
    ): Future[Either[IngestTrackerError, Ingest]] =
      response
  }

  def successfulClient(ingest: Ingest) = new FakeIngestTrackerClient(
    Future.successful(Right(ingest))
  )

  def conflictClient(ingestUpdate: IngestUpdate) = new FakeIngestTrackerClient(
    Future.successful(Left(IngestTrackerConflictError(ingestUpdate)))
  )

  def unknownErrorClient(ingestUpdate: IngestUpdate) =
    new FakeIngestTrackerClient(
      Future.successful(Left(IngestTrackerUnknownError(ingestUpdate, error)))
    )

  def failedFutureClient(error: Throwable = error) =
    new FakeIngestTrackerClient(
      Future.failed(error)
    )

  def withIngestWorker[R](
    queue: Queue = Queue(
      url = "queue://test",
      arn = "arn::queue"
    ),
    ingestTrackerClient: IngestTrackerClient
  )(testWith: TestWith[IngestsWorkerService, R]): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        val service = new IngestsWorkerService(
          workerConfig = createAlpakkaSQSWorkerConfig(queue),
          metricsNamespace = "ingests_worker",
          ingestTrackerClient = ingestTrackerClient
        )

        service.run()

        testWith(service)
      }
    }
}
