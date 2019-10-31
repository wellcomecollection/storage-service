package uk.ac.wellcome.platform.storage.bag_versioner.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.{
  MonitoringClientFixture,
  OperationFixtures
}
import uk.ac.wellcome.platform.storage.bag_versioner.services.{
  BagVersioner,
  BagVersionerWorker
}

import uk.ac.wellcome.json.JsonUtil._

trait BagVersionerFixtures
    extends OperationFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with VersionPickerFixtures {

  private val defaultQueue = Queue(
    url = "default_q",
    arn = "arn::default_q"
  )

  def withBagVersioner[R](testWith: TestWith[BagVersioner, R]): R =
    withVersionPicker { versionPicker =>
      testWith(new BagVersioner(versionPicker))
    }

  def withBagVersionerWorker[R](
    queue: Queue = defaultQueue,
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    stepName: String = randomAlphanumericWithLength()
  )(testWith: TestWith[BagVersionerWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
      withMonitoringClient { implicit monitoringClient =>
        withBagVersioner { bagVersioner =>
          val worker = new BagVersionerWorker(
            config = createAlpakkaSQSWorkerConfig(queue),
            bagVersioner = bagVersioner,
            ingestUpdater = ingestUpdater,
            outgoingPublisher = outgoingPublisher
          )

          worker.run()

          testWith(worker)
        }
      }
    }
}
