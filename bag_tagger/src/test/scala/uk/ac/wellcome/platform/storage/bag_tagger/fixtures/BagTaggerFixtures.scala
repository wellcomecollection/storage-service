package uk.ac.wellcome.platform.storage.bag_tagger.fixtures

import io.circe.Decoder
import org.scalatest.Suite
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.fixtures.{
  OperationFixtures,
  StorageManifestDaoFixture
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageManifestFile
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.storage.bag_tagger.services.{
  ApplyTags,
  BagTaggerWorker,
  TagRules
}
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.tags.s3.NewS3Tags

import scala.concurrent.ExecutionContext.Implicits.global

trait BagTaggerFixtures
    extends OperationFixtures
    with Akka
    with SQS
    with NewS3Fixtures
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
    tagRules: StorageManifest => Map[StorageManifestFile, Map[String, String]] =
      TagRules.chooseTags
  )(
    testWith: TestWith[BagTaggerWorker, R]
  )(implicit decoder: Decoder[BagRegistrationNotification]): R =
    withActorSystem { implicit actorSystem =>
      withFakeMonitoringClient() { implicit monitoringClient =>
        withBagTrackerClient(storageManifestDao = storageManifestDao) {
          trackerClient =>
            val worker = new BagTaggerWorker(
              config = createAlpakkaSQSWorkerConfig(queue),
              metricsNamespace = "bag_tagger",
              bagTrackerClient = trackerClient,
              applyTags = new ApplyTags(s3Tags = new NewS3Tags()),
              tagRules = tagRules
            )

            worker.run()

            testWith(worker)
        }
      }
    }
}
