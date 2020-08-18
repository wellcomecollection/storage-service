package uk.ac.wellcome.platform.archive.bag_tracker.storage

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.storage.{
  NoMaximaValueError,
  NoVersionExistsError,
  VersionAlreadyExistsError,
  WriteError
}

trait StorageManifestDaoTestCases[Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with BagIdGenerators
    with StorageManifestGenerators {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withDao[R](testWith: TestWith[StorageManifestDao, R])(
    implicit context: Context
  ): R

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
                .get(storageManifest.id, version = BagVersion(version))
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

    it("allows putting versions in any order") {
      val storageManifest1 = createStorageManifestWith(version = BagVersion(1))
      val storageManifest2 = storageManifest1.copy(version = BagVersion(2))
      val storageManifest3 = storageManifest1.copy(version = BagVersion(3))

      withContext { implicit context =>
        withDao { dao =>
          dao.put(storageManifest2).right.value shouldBe storageManifest2
          dao.put(storageManifest1).right.value shouldBe storageManifest1
          dao.put(storageManifest3).right.value shouldBe storageManifest3
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

          dao
            .listAllVersions(bagId = storageManifest.id)
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
            .listAllVersions(bagId = storageManifest.id)
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

          dao.listAllVersions(bagId).right.value should have size 7

          // Omitting versions 5 and 6
          dao
            .listVersionsBefore(bagId, before = BagVersion(5))
            .right
            .value should have size 5

          // Omitting versions 3, 4, 5 and 6
          dao
            .listVersionsBefore(bagId, before = BagVersion(3))
            .right
            .value should have size 3
        }
      }
    }

    it("returns an empty list if there are no manifests") {
      withContext { implicit context =>
        withDao { dao =>
          dao.listAllVersions(createBagId).right.value shouldBe empty
        }
      }
    }

    it("finds the latest version") {
      val storageManifest = createStorageManifest

      val manifests = (0 to 5).map { version =>
        storageManifest.copy(version = BagVersion(version))
      }

      withContext { implicit context =>
        withDao { dao =>
          manifests.foreach { manifest =>
            dao.put(manifest) shouldBe a[Right[_, _]]
          }

          dao
            .getLatestVersion(storageManifest.id)
            .right
            .value shouldBe BagVersion(5)
        }
      }
    }

    it("returns a NoMaximaValueError() error if there is no latest version") {
      val bagId = createBagId

      withContext { implicit context =>
        withDao { dao =>
          dao
            .getLatestVersion(bagId)
            .left
            .value shouldBe a[NoMaximaValueError]
        }
      }
    }
  }
}
