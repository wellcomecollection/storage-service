package weco.storage_service.bag_tracker.storage

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.generators.{
  BagIdGenerators,
  StorageManifestGenerators
}
import weco.storage.{
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
        externalIdentifier = storageManifest.info.externalIdentifier,
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
          getResultPostInsert.value shouldBe storageManifest

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
                .value shouldBe manifest
          }
        }
      }
    }

    it("allows writing the same manifest to the same version twice") {
      val storageManifest = createStorageManifest

      withContext { implicit context =>
        withDao { dao =>
          dao.put(storageManifest).value shouldBe storageManifest
          dao.put(storageManifest).value shouldBe storageManifest
        }
      }
    }

    it("blocks putting two different manifests with the same id and version") {
      val storageManifest1 = createStorageManifestWith(version = BagVersion(1))
      val storageManifest2 = createStorageManifestWith(
        space = storageManifest1.space,
        externalIdentifier = storageManifest1.info.externalIdentifier,
        version = BagVersion(1)
      )

      withContext { implicit context =>
        withDao { dao =>
          dao.put(storageManifest1).value shouldBe storageManifest1
          dao
            .put(storageManifest2)
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
          dao.put(storageManifest2).value shouldBe storageManifest2
          dao.put(storageManifest1).value shouldBe storageManifest1
          dao.put(storageManifest3).value shouldBe storageManifest3
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

          dao.listAllVersions(bagId).value should have size 7

          // Omitting versions 5 and 6
          dao
            .listVersionsBefore(bagId, before = BagVersion(5))
            .value should have size 5

          // Omitting versions 3, 4, 5 and 6
          dao
            .listVersionsBefore(bagId, before = BagVersion(3))
            .value should have size 3
        }
      }
    }

    it("returns an empty list if there are no manifests") {
      withContext { implicit context =>
        withDao { dao =>
          dao.listAllVersions(createBagId).value shouldBe empty
        }
      }
    }
  }
}
