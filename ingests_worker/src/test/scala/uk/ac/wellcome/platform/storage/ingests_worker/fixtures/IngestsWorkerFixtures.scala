package uk.ac.wellcome.platform.storage.ingests_worker.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests_tracker.client.IngestTrackerClient
import uk.ac.wellcome.platform.storage.ingests_worker.services.IngestsWorkerService

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestsWorkerFixtures
  extends ScalaFutures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with IngestTrackerFixtures {

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
