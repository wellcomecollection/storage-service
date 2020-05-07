package uk.ac.wellcome.platform.storage.ingests_worker.fixtures

import akka.actor.ActorSystem
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests_worker.services.IngestsWorker

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
                           )
                         )(testWith: TestWith[IngestsWorker, R])(implicit actorSystem: ActorSystem): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      val service = new IngestsWorker(
        workerConfig = createAlpakkaSQSWorkerConfig(queue),
        metricsNamespace = "ingests_worker",
        trackerHost = "http://localhost:8080"
      )

      println("@@AWLC starting ingests worker")
      service.run()

      testWith(service)
    }
}
