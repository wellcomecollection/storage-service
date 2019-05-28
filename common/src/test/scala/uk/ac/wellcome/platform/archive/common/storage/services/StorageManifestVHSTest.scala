package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.storage.DoesNotExistError

class StorageManifestVHSTest
    extends FunSpec
    with Matchers
    with EitherValues
    with StorageManifestGenerators
    with StorageManifestVHSFixture {
  it("allows storing and retrieving a record") {
    val storageManifest = createStorageManifest

    val newStorageManifest = createStorageManifestWith(
      space = storageManifest.space,
      bagInfo = storageManifest.info
    )

    storageManifest.id shouldBe newStorageManifest.id

    val dao = createDao
    val store = createStore

    val vhs = createStorageManifestVHS(dao, store)

    // Empty get

    val getResultPreInsert = vhs.getRecord(storageManifest.id)
    getResultPreInsert.left.value shouldBe a[DoesNotExistError]

    // Insert

    val insertResult = vhs.insertRecord(storageManifest)
    insertResult shouldBe a[Right[_, _]]

    val getResultPostInsert = vhs.getRecord(storageManifest.id)
    getResultPostInsert.right.value shouldBe storageManifest

    // Update

    val updateResult = vhs.updateRecord(newStorageManifest)(_ => newStorageManifest)
    updateResult shouldBe a[Right[_, _]]

    val getResultPostUpdate = vhs.getRecord(storageManifest.id)
    getResultPostUpdate.right.value shouldBe newStorageManifest
  }
}
