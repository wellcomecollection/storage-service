package weco.storage_service.bag_root_finder.fixtures

import weco.pekko.fixtures.Pekko
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.PekkoSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.fixtures.OperationFixtures
import weco.storage_service.bag_root_finder.services.{
  BagRootFinder,
  BagRootFinderWorker
}
import weco.storage.fixtures.S3Fixtures

trait BagRootFinderFixtures
    extends OperationFixtures
    with Pekko
    with PekkoSQSWorkerFixtures
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
        config = createPekkoSQSWorkerConfig(queue),
        bagRootFinder = new BagRootFinder(),
        ingestUpdater = ingestUpdater,
        outgoingPublisher = outgoingPublisher
      )

      worker.run()

      testWith(worker)
    }
}
