package uk.ac.wellcome.platform.archive.bags.async.services

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bags.async.fixtures.UpdateStoredManifestServiceFixture
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class UpdateStoredManifestServiceTest
    extends FunSpec
    with ScalaFutures
    with StorageManifestGenerators
    with UpdateStoredManifestServiceFixture {
  it("returns a successful Right if registering the StorageManifest succeeds") {
    val storageManifest = createStorageManifest
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket: Bucket =>
        withUpdateStoredManifestService(table, bucket) { service =>
          val future = service.updateStoredManifest(storageManifest)

          whenReady(future) { _ =>
            assertStored(table, storageManifest.id.toString, storageManifest)
          }
        }
      }
    }
  }

  it("returns a failed Left if updating VHS fails") {
    withLocalDynamoDbTable { table =>
      withUpdateStoredManifestService(table, Bucket("does-not-exist")) {
        service =>
          val future = service.updateStoredManifest(createStorageManifest)

          whenReady(future.failed) { err =>
            err shouldBe a[AmazonS3Exception]
          }
      }
    }
  }
}
