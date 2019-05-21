package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators

import scala.util.Success

class StorageManifestVHSTest
    extends FunSpec
    with Matchers
    with StorageManifestGenerators
    with StorageManifestVHSFixture {
  it("allows storing and retrieving a record") {
    val storageManifest = createStorageManifest

    val newStorageManifest = createStorageManifestWith(
      space = storageManifest.space,
      bagInfo = storageManifest.info
    )

    storageManifest.id shouldBe newStorageManifest.id

    val dao = createStorageManifestDao
    val store = createStorageManifestStore
    val vhs = createStorageManifestVHS(dao, store)

    vhs.getRecord(storageManifest.id) shouldBe Success(None)

    vhs.insertRecord(storageManifest) shouldBe a[Success[_]]
    vhs.getRecord(storageManifest.id) shouldBe Success(Some(storageManifest))

    vhs.updateRecord(newStorageManifest)(_ => newStorageManifest)
    vhs.getRecord(storageManifest.id) shouldBe Success(Some(newStorageManifest))
  }
}
