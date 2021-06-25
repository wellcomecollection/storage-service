package uk.ac.wellcome.platform.storage.ingests_worker.fixtures

import akka.stream.Materializer

import java.time.Instant
import org.scalatest.concurrent.ScalaFutures
import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.monitoring.memory.MemoryMetrics
import weco.storage.generators.IngestGenerators
import weco.storage_service.ingests.models.Ingest.Succeeded
import weco.storage_service.ingests.models.{
  Ingest,
  IngestID,
  IngestUpdate
}
import weco.storage_service.ingests_tracker.client._
import weco.storage_service.ingests_tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests_worker.services.IngestsWorkerService

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global

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
    response: Future[Either[IngestTrackerUpdateError, Ingest]]
  )(
    implicit val ec: ExecutionContext
  ) extends IngestTrackerClient {
    override def updateIngest(
      ingestUpdate: IngestUpdate
    ): Future[Either[IngestTrackerUpdateError, Ingest]] =
      response

    override def createIngest(
      ingest: Ingest
    ): Future[Either[IngestTrackerCreateError, Unit]] =
      Future.failed(new Throwable("BOOM!"))

    override def getIngest(
      id: IngestID
    ): Future[Either[IngestTrackerGetError, Ingest]] =
      Future.failed(new Throwable("BOOM!"))

    override val client = null
    override implicit val mat: Materializer = null
  }

  def successfulClient(ingest: Ingest) = new FakeIngestTrackerClient(
    Future.successful(Right(ingest))
  )

  def conflictClient(ingestUpdate: IngestUpdate) = new FakeIngestTrackerClient(
    Future.successful(Left(IngestTrackerUpdateConflictError(ingestUpdate)))
  )

  def unknownErrorClient(ingestUpdate: IngestUpdate) =
    new FakeIngestTrackerClient(
      Future.successful(
        Left(IngestTrackerUnknownUpdateError(ingestUpdate, error))
      )
    )

  def failedFutureClient(error: Throwable = error) =
    new FakeIngestTrackerClient(
      Future.failed(error)
    )

  def withIngestWorker[R](
    queue: Queue = dummyQueue,
    ingestTrackerClient: IngestTrackerClient
  )(testWith: TestWith[IngestsWorkerService, R]): R =
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val service = new IngestsWorkerService(
        workerConfig = createAlpakkaSQSWorkerConfig(queue),
        metricsNamespace = "ingests_worker",
        ingestTrackerClient = ingestTrackerClient
      )

      service.run()

      testWith(service)
    }
}
