package uk.ac.wellcome.platform.storage.bag_root_finder.fixtures

import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage.fixtures.OperationFixtures
import uk.ac.wellcome.platform.storage.bag_root_finder.services.{
  BagRootFinder,
  BagRootFinderWorker
}
import weco.storage.fixtures.S3Fixtures

trait BagRootFinderFixtures
    extends OperationFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with S3Fixtures {

  def withWorkerService[R](
    queue: Queue,
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    stepName: String = createStepName
  )(testWith: TestWith[BagRootFinderWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val worker = new BagRootFinderWorker(
        config = createAlpakkaSQSWorkerConfig(queue),
        bagRootFinder = new BagRootFinder(),
        ingestUpdater = ingestUpdater,
        outgoingPublisher = outgoingPublisher,
        metricsNamespace = "bag_root_finder"
      )

      worker.run()

      testWith(worker)
    }
}
