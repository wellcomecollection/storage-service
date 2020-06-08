package uk.ac.wellcome.platform.storage.bag_tagger.fixtures

import io.circe.Decoder
import org.scalatest.Suite
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.storage.bag_tagger.services.BagTaggerWorker

import scala.concurrent.ExecutionContext.Implicits.global

trait BagTaggerFixtures
    extends OperationFixtures
    with Akka
    with SQS
    with AlpakkaSQSWorkerFixtures { this: Suite =>

  def withWorkerService[R](
    queue: Queue,
    outgoing: MemoryMessageSender,
  )(
    testWith: TestWith[BagTaggerWorker, R]
  )(implicit decoder: Decoder[BagRegistrationNotification]): R =
    withActorSystem { implicit actorSystem =>
      withFakeMonitoringClient() { implicit monitoringClient =>
        val worker = new BagTaggerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          metricsNamespace = "bag_tagger"
        )

        testWith(worker)
      }
    }
}
