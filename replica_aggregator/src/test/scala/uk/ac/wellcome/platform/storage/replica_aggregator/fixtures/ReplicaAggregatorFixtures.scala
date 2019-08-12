package uk.ac.wellcome.platform.storage.replica_aggregator.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.{MonitoringClientFixture, OperationFixtures}
import uk.ac.wellcome.platform.storage.replica_aggregator.services.{ReplicaAggregator, ReplicaAggregatorWorker}

trait ReplicaAggregatorFixtures
    extends OperationFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture {

  private val defaultQueue = Queue(
    url = "default_q",
    arn = "arn::default_q"
  )

  def withReplicaAggregator[R](testWith: TestWith[ReplicaAggregator, R]): R =
    withReplicaAggregator { replicaAggregator =>
      withReplicaAggregator(replicaAggregator) { replicaAggregator =>
        testWith(replicaAggregator)
      }
    }

  def withReplicaAggregator[R](
    replicaAggregator: ReplicaAggregator
  )(testWith: TestWith[ReplicaAggregator, R]): R = {
    val ReplicaAggregator = new ReplicaAggregator()

    testWith(ReplicaAggregator)
  }

  def withReplicaAggregatorWorker[R](
    queue: Queue = defaultQueue,
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    stepName: String = randomAlphanumericWithLength()
  )(testWith: TestWith[ReplicaAggregatorWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
      withMonitoringClient { implicit monitoringClient =>
        withReplicaAggregator { replicaAggregator =>
          val worker = new ReplicaAggregatorWorker(
            config = createAlpakkaSQSWorkerConfig(queue),
            replicaAggregator = replicaAggregator,
            ingestUpdater = ingestUpdater,
            outgoingPublisher = outgoingPublisher
          )

          worker.run()

          testWith(worker)
        }
      }
    }
}
