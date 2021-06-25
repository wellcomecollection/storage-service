package uk.ac.wellcome.platform.storage.bag_tagger.fixtures

import io.circe.Decoder
import org.scalatest.Suite
import weco.fixtures.TestWith
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.{
  BagTrackerFixtures,
  StorageManifestDaoFixture
}
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import weco.storage.BagRegistrationNotification
import weco.storage.fixtures.OperationFixtures
import weco.storage_service.storage.models.{
  StorageManifest,
  StorageManifestFile
}
import uk.ac.wellcome.platform.storage.bag_tagger.services.{
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
      visibilityTimeout = 1
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
            metricsNamespace = "bag_tagger",
            bagTrackerClient = trackerClient,
            applyTags = applyTags,
            tagRules = tagRules
          )

          worker.run()

          testWith(worker)
      }
    }
}
