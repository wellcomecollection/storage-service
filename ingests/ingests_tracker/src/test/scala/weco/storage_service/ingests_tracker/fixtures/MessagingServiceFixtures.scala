package weco.storage_service.ingests_tracker.fixtures

import org.scalatest.concurrent.ScalaFutures
import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.ingests_tracker.services.{
  CallbackNotificationService,
  MessagingService
}

trait MessagingServiceFixtures
    extends ScalaFutures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with IngestTrackerFixtures {

  def withMessagingService[R](
    callbackSender: MemoryMessageSender = new MemoryMessageSender(),
    ingestsSender: MemoryMessageSender = new MemoryMessageSender()
  )(
    testWith: TestWith[
      (
        MemoryMessageSender,
        MemoryMessageSender,
        MessagingService[String, String]
      ),
      R
    ]
  ): R = {
    val callbackNotificationService =
      new CallbackNotificationService(callbackSender)

    val messagingService = new MessagingService(
      callbackNotificationService = callbackNotificationService,
      updatedIngestsMessageSender = ingestsSender
    )

    val out = (callbackSender, ingestsSender, messagingService)

    testWith(out)
  }
}
