package uk.ac.wellcome.platform.storage.replica_aggregator.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{
  AggregatorInternalRecord,
  ReplicaPath
}
import uk.ac.wellcome.platform.storage.replica_aggregator.services.{
  ReplicaAggregator,
  ReplicaAggregatorWorker,
  ReplicaCounter
}
import uk.ac.wellcome.storage.store.VersionedStore
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

import scala.concurrent.ExecutionContext.Implicits.global

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
    stepName: String = randomAlphanumericWithLength(),
    expectedReplicaCount: Int = 1
  )(testWith: TestWith[ReplicaAggregatorWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      withFakeMonitoringClient() { implicit monitoringClient =>
        val worker = new ReplicaAggregatorWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          replicaAggregator = new ReplicaAggregator(versionedStore),
          replicaCounter =
            new ReplicaCounter(expectedReplicaCount = expectedReplicaCount),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher,
          metricsNamespace = "replica_aggregator"
        )

        worker.run()

        testWith(worker)
      }
    }
}
