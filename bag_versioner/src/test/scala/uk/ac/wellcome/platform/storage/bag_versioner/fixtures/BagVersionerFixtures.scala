package uk.ac.wellcome.platform.storage.bag_versioner.fixtures

import java.util.UUID

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.storage.bag_versioner.services.{
  BagVersioner,
  BagVersionerWorker
}
import uk.ac.wellcome.storage.locking.LockDao

trait BagVersionerFixtures
    extends OperationFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with VersionPickerFixtures {

  def withBagVersioner[R](
    dao: LockDao[String, UUID]
  )(testWith: TestWith[BagVersioner, R]): R =
    withVersionPicker(dao) { versionPicker =>
      testWith(new BagVersioner(versionPicker))
    }

  def withBagVersioner[R](testWith: TestWith[BagVersioner, R]): R =
    withVersionPicker { versionPicker =>
      testWith(new BagVersioner(versionPicker))
    }

  def withBagVersionerWorker[R](
    queue: Queue = dummyQueue,
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    stepName: String = createStepName
  )(testWith: TestWith[BagVersionerWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      withBagVersioner { bagVersioner =>
        val worker = new BagVersionerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          bagVersioner = bagVersioner,
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher,
          metricsNamespace = "bag_versioner"
        )

        worker.run()

        testWith(worker)
      }
    }
}
