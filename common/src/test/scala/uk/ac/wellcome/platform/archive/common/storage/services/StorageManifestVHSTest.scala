package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators

class StorageManifestVHSTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with StorageManifestGenerators
    with StorageManifestVHSFixture {
  it("allows storing and retrieving a record") {
    val storageManifest = createStorageManifest

    val newStorageManifest = createStorageManifestWith(
      space = storageManifest.space,
      bagInfo = storageManifest.info
    )

    storageManifest.id shouldBe newStorageManifest.id

    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withStorageManifestVHS(table, bucket) { vhs =>
          val futureGet = vhs.getRecord(storageManifest.id)
          whenReady(futureGet) { result =>
            result shouldBe None
          }

          val futureInsert = vhs.insertRecord(storageManifest)
          whenReady(futureInsert) { _ =>
            getStorageManifest(table, storageManifest.id) shouldBe storageManifest

            val future =
              vhs.updateRecord(newStorageManifest)(_ => newStorageManifest)
            whenReady(future) { _ =>
              getStorageManifest(table, storageManifest.id) shouldBe newStorageManifest

              val future = vhs.getRecord(storageManifest.id)
              whenReady(future) { retrievedManifest =>
                retrievedManifest shouldBe Some(newStorageManifest)
              }
            }
          }
        }
      }
    }
  }
}
