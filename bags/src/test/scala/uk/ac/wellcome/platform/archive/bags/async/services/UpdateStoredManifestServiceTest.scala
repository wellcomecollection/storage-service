package uk.ac.wellcome.platform.archive.bags.async.services

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bags.async.fixtures.UpdateStoredManifestServiceFixture
import uk.ac.wellcome.platform.archive.bags.generators.StorageManifestGenerators
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class UpdateStoredManifestServiceTest extends FunSpec with ScalaFutures with StorageManifestGenerators with UpdateStoredManifestServiceFixture {
  it("returns a successful Right if registering the StorageManifest succeeds") {
    val storageManifest = createStorageManifest
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket: Bucket =>
        withUpdateStoredManifestService(table, bucket) { service =>
          val future = service.updateStoredManifest(storageManifest)

          whenReady(future) { result =>
            result.isSuccess shouldBe true
            assertStored(table, storageManifest.id.toString, storageManifest)
          }
        }
      }
    }
  }

  it("returns a failed Left if updating VHS fails") {
    withLocalDynamoDbTable { table =>
      withUpdateStoredManifestService(table, Bucket("does-not-exist")) { service =>
        val future = service.updateStoredManifest(createStorageManifest)

        whenReady(future) { result =>
          result.isFailure shouldBe true
          val err = result.failed.get
          err shouldBe a[AmazonS3Exception]
        }
      }
    }
  }
}
