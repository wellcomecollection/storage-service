package uk.ac.wellcome.platform.archive.bags.async.services

import com.amazonaws.services.sns.model.AmazonSNSException
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.bags.async.fixtures.UpdateStoredManifestFixture
import uk.ac.wellcome.platform.archive.bags.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models.Progress
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class UpdateStoredManifestServiceTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with ProgressUpdateAssertions
    with StorageManifestGenerators
    with UpdateStoredManifestFixture {
  it("puts a new StorageManifest in VHS") {
    val archiveRequestId = randomUUID
    val storageManifest = createStorageManifest

    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { progressTopic =>
          withUpdateStoredManifestService(table, bucket, progressTopic) {
            service =>
              val future = service.updateManifest(
                archiveRequestId,
                storageManifest = storageManifest)

              whenReady(future) { _ =>
                assertStored(
                  table,
                  storageManifest.id.toString,
                  storageManifest)

                assertTopicReceivesProgressStatusUpdate(
                  archiveRequestId,
                  progressTopic,
                  status = Progress.Completed,
                  expectedBag = Some(storageManifest.id)) { events =>
                  events should have size 1
                  events.head.description shouldBe "Bag registered successfully"
                }
              }
          }
        }
      }
    }
  }

  it("sends a failed ProgressUpdate if updating VHS fails") {
    val archiveRequestId = randomUUID
    val storageManifest = createStorageManifest

    val table = Table("does-not-exist", index = "does-not-exist")
    val bucket = Bucket("does-not-exist")

    withLocalSnsTopic { progressTopic =>
      withUpdateStoredManifestService(table, bucket, progressTopic) { service =>
        val future = service.updateManifest(
          archiveRequestId,
          storageManifest = storageManifest)

        whenReady(future) { _ =>
          assertTopicReceivesProgressStatusUpdate(
            archiveRequestId,
            progressTopic,
            status = Progress.Failed,
            expectedBag = Some(storageManifest.id)) { events =>
            events should have size 1
            events.head.description shouldBe "Failed to register bag"
          }
        }
      }
    }
  }

  it("returns a failed Future if publishing to SNS fails") {
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        val progressTopic = Topic("does-not-exist")
        withUpdateStoredManifestService(table, bucket, progressTopic) {
          service =>
            val future = service.updateManifest(
              randomUUID,
              storageManifest = createStorageManifest)

            whenReady(future.failed) { err =>
              err shouldBe a[AmazonSNSException]
            }
        }
      }
    }
  }
}
