package uk.ac.wellcome.platform.archive.bags.async.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.bags.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.bags.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

class UpdateStoredManifestServiceTest extends FunSpec with Matchers with ScalaFutures with ProgressUpdateAssertions with SNS with StorageManifestGenerators with StorageManifestVHSFixture {
  it("puts a new StorageManifest in VHS") {
    val archiveRequestId = randomUUID
    val storageManifest = createStorageManifest

    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { progressTopic =>
          withUpdateStoredManifestService(table, bucket, progressTopic) { service =>
            val future = service.updateManifest(archiveRequestId, storageManifest = storageManifest)

            whenReady(future) { _ =>
              assertStored(table, storageManifest.id.toString, storageManifest)

              assertTopicReceivesProgressEventUpdate(archiveRequestId, progressTopic) { events =>
                events should have size 1
                events.head.description shouldBe "Bag registered successfully"
              }
            }
          }
        }
      }
    }
  }

  private def withUpdateStoredManifestService[R](table: Table, bucket: Bucket, topic: Topic)(testWith: TestWith[UpdateStoredManifestService, R]): R =
    withStorageManifestVHS(table, bucket) { vhs =>
      withSNSWriter(topic) { progressSnsWriter =>
        val service = new UpdateStoredManifestService(
          vhs = vhs,
          progressSnsWriter = progressSnsWriter
        )
        testWith(service)
      }
    }
}
