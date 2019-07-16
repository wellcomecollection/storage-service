package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.storage.{
  NoVersionExistsError,
  VersionAlreadyExistsError,
  WriteError
}

trait StorageManifestDaoTestCases[Context]
    extends FunSpec
    with Matchers
    with EitherValues
    with StorageManifestGenerators {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withDao[R](testWith: TestWith[StorageManifestDao, R])(
    implicit context: Context): R

  it("allows storing and retrieving a record") {
    val storageManifest = createStorageManifest

    val newStorageManifest = createStorageManifestWith(
      space = storageManifest.space,
      bagInfo = storageManifest.info,
      version = storageManifest.version
    )

    storageManifest.id shouldBe newStorageManifest.id

    withContext { implicit context =>
      withDao { dao =>
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
    }
  }

  it("blocks putting two manifests with the same version") {
    val storageManifest = createStorageManifest

    withContext { implicit context =>
      withDao { dao =>
        dao.put(storageManifest).right.value shouldBe storageManifest
        dao
          .put(storageManifest)
          .left
          .value shouldBe a[VersionAlreadyExistsError]
      }
    }
  }
}
