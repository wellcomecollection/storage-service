package weco.storage_service.ingests_tracker.fixtures

import org.scalatest.concurrent.ScalaFutures
import weco.pekko.fixtures.Pekko
import weco.fixtures.TestWith
import weco.messaging.fixtures.worker.PekkoSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.ingests_tracker.services.{
  CallbackNotificationService,
  MessagingService
}

trait MessagingServiceFixtures
    extends ScalaFutures
    with Pekko
    with PekkoSQSWorkerFixtures
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
