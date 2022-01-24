package weco.storage_service.replica_aggregator.fixtures

import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.fixtures.OperationFixtures
import weco.storage_service.replica_aggregator.models.{
  AggregatorInternalRecord,
  ReplicaPath
}
import weco.storage_service.replica_aggregator.services.{
  ReplicaAggregator,
  ReplicaAggregatorWorker,
  ReplicaCounter
}
import weco.storage.store.VersionedStore
import weco.storage.store.memory.MemoryVersionedStore

trait ReplicaAggregatorFixtures
    extends OperationFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures {

  def withReplicaAggregatorWorker[R](
    queue: Queue = dummyQueue,
    versionedStore: VersionedStore[ReplicaPath, Int, AggregatorInternalRecord] =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      ),
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender = new MemoryMessageSender(),
    stepName: String = createStepName,
    expectedReplicaCount: Int = 1
  )(testWith: TestWith[ReplicaAggregatorWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val worker = new ReplicaAggregatorWorker(
        config = createAlpakkaSQSWorkerConfig(queue),
        replicaAggregator = new ReplicaAggregator(versionedStore),
        replicaCounter =
          new ReplicaCounter(expectedReplicaCount = expectedReplicaCount),
        ingestUpdater = ingestUpdater,
        outgoingPublisher = outgoingPublisher
      )

      worker.run()

      testWith(worker)
    }
}
