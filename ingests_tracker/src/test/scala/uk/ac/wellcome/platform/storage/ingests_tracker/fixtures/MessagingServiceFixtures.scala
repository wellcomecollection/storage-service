package uk.ac.wellcome.platform.storage.ingests_tracker.fixtures

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.storage.ingests_tracker.services.{CallbackNotificationService, MessagingService}

import scala.concurrent.ExecutionContext.Implicits.global

trait MessagingServiceFixtures
    extends ScalaFutures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with IngestTrackerFixtures {

  def withMessagingService[R](
                               callbackSender: MemoryMessageSender = new MemoryMessageSender(),
                               ingestsSender: MemoryMessageSender = new MemoryMessageSender()
                             )(testWith: TestWith[(MemoryMessageSender, MemoryMessageSender, MessagingService[String, String]), R]): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        val callbackNotificationService =
          new CallbackNotificationService(callbackSender)

        val messagingService = new MessagingService(
          callbackNotificationService = callbackNotificationService,
          updatedIngestsMessageSender = ingestsSender,
        )

        val out = (callbackSender, ingestsSender, messagingService)

        testWith(out)
      }
    }
}
