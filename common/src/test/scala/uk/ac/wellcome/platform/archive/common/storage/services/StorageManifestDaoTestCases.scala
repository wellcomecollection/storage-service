package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.{BagIdGenerators, StorageManifestGenerators}
import uk.ac.wellcome.storage.{NoVersionExistsError, VersionAlreadyExistsError, WriteError}

trait StorageManifestDaoTestCases[Context]
    extends FunSpec
    with Matchers
    with EitherValues
    with BagIdGenerators
    with StorageManifestGenerators {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withDao[R](testWith: TestWith[StorageManifestDao, R])(
    implicit context: Context): R

  describe("behaves as a StorageManifestDao") {
    it("allows storing and retrieving a manifest") {
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

    it("allows storing multiple versions of the same manifest") {
      val storageManifest = createStorageManifest

      val manifests = (0 to 5).map { version =>
        storageManifest.copy(
          createdDate = randomInstant,
          version = BagVersion(version)
        )
      }

      withContext { implicit context =>
        withDao { dao =>
          manifests.zipWithIndex.foreach {
            case (manifest, version) =>
              dao.put(manifest) shouldBe a[Right[_, _]]
              dao
                .get(storageManifest.id, version = version)
                .right
                .value shouldBe manifest
          }
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

    it("retrieves a list of versions") {
      val storageManifest = createStorageManifest

      val manifests = (0 to 5).map { version =>
        storageManifest.copy(
          createdDate = randomInstant,
          version = BagVersion(version)
        )
      }

      withContext { implicit context =>
        withDao { dao =>
          manifests.foreach { manifest =>
            dao.put(manifest) shouldBe a[Right[_, _]]
          }

          println(dao.listVersions(bagId = storageManifest.id))
          dao
            .listVersions(bagId = storageManifest.id)
            .right
            .value should contain theSameElementsAs manifests
        }
      }
    }

    it("retrieves a list of versions in descending order") {
      val storageManifest = createStorageManifest

      val manifests = (0 to 6).map { version =>
        storageManifest.copy(
          createdDate = randomInstant,
          version = BagVersion(version)
        )
      }

      withContext { implicit context =>
        withDao { dao =>
          manifests.foreach { manifest =>
            dao.put(manifest) shouldBe a[Right[_, _]]
          }

          dao
            .listVersions(bagId = storageManifest.id)
            .right
            .value shouldBe manifests.reverse
        }
      }
    }

    it("only returns versions less than the 'before' parameter") {
      val storageManifest = createStorageManifest

      val manifests = (0 to 6).map { version =>
        storageManifest.copy(
          createdDate = randomInstant,
          version = BagVersion(version)
        )
      }

      withContext { implicit context =>
        withDao { dao =>
          manifests.foreach { manifest =>
            dao.put(manifest) shouldBe a[Right[_, _]]
          }

          val bagId = storageManifest.id

          dao.listVersions(bagId).right.value should have size 7

          // Omitting versions 5 and 6
          dao.listVersions(bagId, before = 5).right.value should have size 5

          // Omitting versions 3, 4, 5 and 6
          dao.listVersions(bagId, before = 3).right.value should have size 3
        }
      }
    }

    it("returns an empty list if there are no manifests") {
      withContext { implicit context =>
        withDao { dao =>
          dao.listVersions(createBagId).right.value shouldBe empty
        }
      }
    }
  }
}
