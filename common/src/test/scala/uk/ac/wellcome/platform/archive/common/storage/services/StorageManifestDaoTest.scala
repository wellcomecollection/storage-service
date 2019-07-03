package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.storage.{
  NoVersionExistsError,
  VersionAlreadyExistsError,
  WriteError
}

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
      bagInfo = storageManifest.info,
      version = storageManifest.version
    )

    storageManifest.id shouldBe newStorageManifest.id

    implicit val index: StorageManifestIndex = createIndex
    implicit val typedStore: StorageManifestTypedStore = createTypedStore

    val dao: StorageManifestDao = createStorageManifestDao

    // Empty get

    val getResultPreInsert = dao.getLatest(storageManifest.id)
    getResultPreInsert.left.value shouldBe a[NoVersionExistsError]

    // Insert

    val insertResult = dao.put(storageManifest)
    insertResult shouldBe a[Right[_, _]]

    val getResultPostInsert = dao.getLatest(storageManifest.id)
    getResultPostInsert.right.value shouldBe storageManifest

    // Update

    val updateResult = dao.put(newStorageManifest)
    updateResult.left.value shouldBe a[WriteError]
  }

  it("stores a record under the appropriate ID and version") {
    implicit val index: StorageManifestIndex = createIndex

    val dao: StorageManifestDao = createStorageManifestDao

    val storageManifest = createStorageManifestWith(
      version = 2
    )

    val newStorageManifest = createStorageManifestWith(
      space = storageManifest.space,
      bagInfo = storageManifest.info,
      version = 3
    )

    dao.put(storageManifest).right.value shouldBe storageManifest
    dao.put(newStorageManifest).right.value shouldBe newStorageManifest

    index.entries.size shouldBe 2

    dao
      .get(id = storageManifest.id, version = storageManifest.version)
      .right
      .value shouldBe storageManifest
    dao
      .get(id = storageManifest.id, version = newStorageManifest.version)
      .right
      .value shouldBe newStorageManifest
  }

  it("blocks putting two manifests with the same version") {
    val dao: StorageManifestDao = createStorageManifestDao

    val storageManifest = createStorageManifest

    dao.put(storageManifest).right.value shouldBe storageManifest
    dao.put(storageManifest).left.value shouldBe a[VersionAlreadyExistsError]
  }
}
