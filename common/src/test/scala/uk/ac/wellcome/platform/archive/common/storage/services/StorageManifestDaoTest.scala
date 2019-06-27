package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.storage.DoesNotExistError

class StorageManifestDaoTest
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

    val index = createIndex
    val typedStore = createTypedStore

    val dao: StorageManifestDao =
      createStorageManifestDao(index, typedStore)

    // Empty get

    val getResultPreInsert = dao.get(storageManifest.id)
    getResultPreInsert.left.value shouldBe a[DoesNotExistError]

    // Insert

    val insertResult = dao.put(storageManifest)
    insertResult shouldBe a[Right[_, _]]

    val getResultPostInsert = dao.get(storageManifest.id)
    getResultPostInsert.right.value shouldBe storageManifest

    // Update

    val updateResult = dao.put(newStorageManifest)
    updateResult shouldBe a[Right[_, _]]

    val getResultPostUpdate = dao.get(storageManifest.id)
    getResultPostUpdate.right.value shouldBe newStorageManifest
  }
}
