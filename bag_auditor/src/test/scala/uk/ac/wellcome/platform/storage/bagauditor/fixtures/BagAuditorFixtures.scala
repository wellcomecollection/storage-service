package uk.ac.wellcome.platform.storage.bagauditor.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  MonitoringClientFixture,
  OperationFixtures
}
import uk.ac.wellcome.platform.storage.bagauditor.services.{
  BagAuditor,
  BagAuditorWorker
}
import uk.ac.wellcome.platform.storage.bagauditor.versioning.VersionPicker

trait BagAuditorFixtures
    extends BagLocationFixtures
    with OperationFixtures
    with Akka
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
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    stepName: String = randomAlphanumericWithLength()
  )(testWith: TestWith[BagAuditorWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
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
