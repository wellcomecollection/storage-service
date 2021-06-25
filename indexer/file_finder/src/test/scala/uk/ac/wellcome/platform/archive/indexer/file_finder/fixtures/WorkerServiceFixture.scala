package uk.ac.wellcome.platform.archive.indexer.file_finder.fixtures

import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_tracker.client.BagTrackerClient
import weco.storage_service.bag_tracker.fixtures.{
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
