package weco.storage_service.bag_tagger.fixtures

import scala.concurrent.duration._

import io.circe.Decoder
import org.scalatest.Suite

import weco.fixtures.TestWith
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_tracker.fixtures.{
  BagTrackerFixtures,
  StorageManifestDaoFixture
}
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.BagRegistrationNotification
import weco.storage_service.fixtures.OperationFixtures
import weco.storage_service.storage.models.{
  StorageManifest,
  StorageManifestFile
}
import weco.storage_service.bag_tagger.services.{
  ApplyTags,
  BagTaggerWorker,
  TagRules
}
import weco.storage.fixtures.S3Fixtures

trait BagTaggerFixtures
    extends OperationFixtures
    with S3Fixtures
    with BagTrackerFixtures
    with StorageManifestDaoFixture
    with AlpakkaSQSWorkerFixtures { this: Suite =>

  def withWorkerService[R](
    queue: Queue = Queue(
      url = "q://bag-tagger-tests",
      arn = "arn::bag-tagger-tests",
      visibilityTimeout = 1 seconds
    ),
    storageManifestDao: StorageManifestDao = createStorageManifestDao(),
    applyTags: ApplyTags = ApplyTags(),
    tagRules: StorageManifest => Map[StorageManifestFile, Map[String, String]] =
      TagRules.chooseTags
  )(
    testWith: TestWith[BagTaggerWorker, R]
  )(implicit decoder: Decoder[BagRegistrationNotification]): R =
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      withBagTrackerClient(storageManifestDao = storageManifestDao) {
        trackerClient =>
          val worker = new BagTaggerWorker(
            config = createAlpakkaSQSWorkerConfig(queue),
            bagTrackerClient = trackerClient,
            applyTags = applyTags,
            tagRules = tagRules
          )

          worker.run()

          testWith(worker)
      }
    }
}
