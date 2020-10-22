package uk.ac.wellcome.platform.archive.indexer.file_finder.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.bag_tracker.client.BagTrackerClient
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.{
  BagTrackerFixtures,
  StorageManifestDaoFixture
}
import uk.ac.wellcome.platform.archive.indexer.file_finder.FileFinderWorker

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends AlpakkaSQSWorkerFixtures
    with BagTrackerFixtures
    with StorageManifestDaoFixture {
  def withWorkerService[R](
    queue: Queue = Queue("q", "arn::q", visibilityTimeout = 1),
    messageSender: MemoryMessageSender,
    bagTrackerClient: BagTrackerClient,
    batchSize: Int = 100
  )(
    testWith: TestWith[FileFinderWorker, R]
  ): R =
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val service = new FileFinderWorker(
        config = createAlpakkaSQSWorkerConfig(queue),
        bagTrackerClient = bagTrackerClient,
        metricsNamespace = s"metrics-${randomAlphanumeric()}",
        messageSender = messageSender,
        batchSize = batchSize
      )

      service.run()

      testWith(service)
    }

  def withWorkerService[R](
    testWith: TestWith[FileFinderWorker, R]
  ): R = {
    val dao = createStorageManifestDao()

    withBagTrackerClient(dao) { bagTrackerClient =>
      withWorkerService(
        messageSender = new MemoryMessageSender(),
        bagTrackerClient = bagTrackerClient
      ) {
        testWith(_)
      }
    }
  }
}
