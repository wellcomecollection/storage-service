package weco.storage_service.bag_versioner.fixtures

import java.util.UUID

import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.fixtures.OperationFixtures
import weco.storage_service.bag_versioner.services.{
  BagVersioner,
  BagVersionerWorker
}
import weco.storage.locking.LockDao

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
        )

        worker.run()

        testWith(worker)
      }
    }
}
