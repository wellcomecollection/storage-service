package uk.ac.wellcome.platform.storage.bagauditor.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, MonitoringClientFixture, OperationFixtures, RandomThings}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.storage.bagauditor.services.{BagAuditor, BagAuditorWorker}
import uk.ac.wellcome.platform.storage.bagauditor.versioning.VersionPicker
import uk.ac.wellcome.storage.fixtures.S3

trait BagAuditorFixtures
    extends S3
    with RandomThings
    with BagLocationFixtures
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with VersionPickerFixtures {

  private val defaultQueue = Queue(
    url = "default_q",
    arn = "arn::default_q"
  )

  def withBagAuditor[R](testWith: TestWith[BagAuditor, R]): R =
    withVersionPicker { versionPicker =>
      withBagAuditor(versionPicker) { bagAuditor =>
        testWith(bagAuditor)
      }
    }

  def withBagAuditor[R](versionPicker: VersionPicker)(
    testWith: TestWith[BagAuditor, R]): R = {
    val bagAuditor = new BagAuditor(versionPicker)

    testWith(bagAuditor)
  }

  def withAuditorWorker[R](
    queue: Queue = defaultQueue,
    ingestsMessageSender: MessageSender[String],
    outgoingMessageSender: MessageSender[String]
  )(testWith: TestWith[BagAuditorWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = new IngestUpdater[String](
        stepName = "auditing bag",
        messageSender = ingestsMessageSender
      )

      val outgoingPublisher = new OutgoingPublisher[String](
        operationName = "auditing bag",
        messageSender = outgoingMessageSender
      )

      withMonitoringClient { implicit monitoringClient =>
        withBagAuditor { bagAuditor =>
          val worker = new BagAuditorWorker(
            alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
            bagAuditor = bagAuditor,
            ingestUpdater = ingestUpdater,
            outgoingPublisher = outgoingPublisher
          )

          worker.run()

          testWith(worker)
        }
      }
    }
}
