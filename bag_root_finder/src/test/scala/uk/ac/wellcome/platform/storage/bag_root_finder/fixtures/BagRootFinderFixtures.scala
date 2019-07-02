package uk.ac.wellcome.platform.storage.bag_root_finder.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.{
  MonitoringClientFixture,
  OperationFixtures
}
import uk.ac.wellcome.platform.storage.bag_root_finder.services.{
  BagRootFinder,
  BagRootFinderWorker
}
import uk.ac.wellcome.storage.fixtures.S3Fixtures

trait BagRootFinderFixtures
    extends OperationFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with S3Fixtures {

  def withWorkerService[R](
    queue: Queue,
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    stepName: String = randomAlphanumericWithLength()
  )(testWith: TestWith[BagRootFinderWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
      withMonitoringClient { implicit monitoringClient =>
        val worker = new BagRootFinderWorker(
          alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
          bagRootFinder = new BagRootFinder(),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher
        )

        worker.run()

        testWith(worker)
      }
    }
}
