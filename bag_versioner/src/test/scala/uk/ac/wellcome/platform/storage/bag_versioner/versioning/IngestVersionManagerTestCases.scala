package uk.ac.wellcome.platform.storage.bag_versioner.versioning

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  UpdateIngestType
}
import uk.ac.wellcome.storage.NoMaximaValueError

trait IngestVersionManagerTestCases[DaoImpl <: IngestVersionManagerDao, Context]
    extends FunSpec
    with Matchers
    with EitherValues
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withDao[R](testWith: TestWith[DaoImpl, R])(implicit context: Context): R

  def withBrokenLookupExistingVersionDao[R](testWith: TestWith[DaoImpl, R])(
    implicit context: Context
  ): R
  def withBrokenLookupLatestVersionForDao[R](testWith: TestWith[DaoImpl, R])(
    implicit context: Context
  ): R
  def withBrokenStoreNewVersionDao[R](testWith: TestWith[DaoImpl, R])(
    implicit context: Context
  ): R

  def withManager[R](dao: DaoImpl)(testWith: TestWith[IngestVersionManager, R])(
    implicit context: Context
  ): R

  def withManager[R](
    testWith: TestWith[IngestVersionManager, R]
  )(implicit context: Context): R =
    withDao { dao =>
      withManager(dao) { manager =>
        testWith(manager)
      }
    }

  describe("behaves as an ingest version manager") {
    it("assigns version 1 if it hasn't seen this external ID before") {
      withContext { implicit context =>
        withManager { manager =>
          manager
            .assignVersion(
              externalIdentifier = createExternalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              storageSpace = createStorageSpace
            )
            .right
            .value shouldBe BagVersion(1)
        }
      }
    }

    it("assigns increasing versions if it sees newer ingest dates each time") {
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace

      withContext { implicit context =>
        withManager { manager =>
          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(1),
              ingestType = CreateIngestType,
              storageSpace = storageSpace
            )
            .right
            .value shouldBe BagVersion(1)

          (2 to 5).map { version =>
            manager
              .assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.ofEpochSecond(version),
                ingestType = UpdateIngestType,
                storageSpace = storageSpace
              )
              .right
              .value shouldBe BagVersion(version)
          }
        }
      }
    }

    it("always assigns the same version to a given ingest ID") {
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace

      withContext { implicit context =>
        withManager { manager =>
          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(0),
              ingestType = CreateIngestType,
              storageSpace = storageSpace
            )

          val ingestIds = (1 to 5).map { idx =>
            (idx, createIngestID)
          }

          val assignedVersions = ingestIds.map {
            case (idx, ingestId) =>
              val version = manager
                .assignVersion(
                  externalIdentifier = externalIdentifier,
                  ingestId = ingestId,
                  ingestDate = Instant.ofEpochSecond(idx),
                  ingestType = UpdateIngestType,
                  storageSpace = storageSpace
                )
                .right
                .value

              (idx, ingestId, version)
          }

          assignedVersions.foreach {
            case (idx, ingestId, version) =>
              manager
                .assignVersion(
                  externalIdentifier = externalIdentifier,
                  ingestId = ingestId,
                  ingestDate = Instant.ofEpochSecond(idx),
                  ingestType = CreateIngestType,
                  storageSpace = storageSpace
                )
                .right
                .value shouldBe version
          }
        }
      }
    }

    it(
      "assigns independent versions to different external IDs in the same space"
    ) {
      withContext { implicit context =>
        withManager { manager =>
          val storageSpace = createStorageSpace

          manager
            .assignVersion(
              externalIdentifier = createExternalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              storageSpace = storageSpace
            )
            .right
            .value shouldBe BagVersion(1)

          manager
            .assignVersion(
              externalIdentifier = createExternalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              storageSpace = storageSpace
            )
            .right
            .value shouldBe BagVersion(1)
        }
      }
    }

    it(
      "assigns independent versions to the same external ID in different spaces"
    ) {
      withContext { implicit context =>
        withManager { manager =>
          val externalIdentifier = createExternalIdentifier

          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              storageSpace = createStorageSpace
            )
            .right
            .value shouldBe BagVersion(1)

          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              storageSpace = createStorageSpace
            )
            .right
            .value shouldBe BagVersion(1)
        }
      }
    }

    it("errors if the external ID in the request doesn't match the database") {
      withContext { implicit context =>
        withManager { manager =>
          val ingestId = createIngestID
          val storageSpace = createStorageSpace

          val storedExternalIdentifier = createExternalIdentifier

          manager.assignVersion(
            externalIdentifier = storedExternalIdentifier,
            ingestId = ingestId,
            ingestDate = Instant.now,
            ingestType = CreateIngestType,
            storageSpace = storageSpace
          )

          val newExternalIdentifier = createExternalIdentifier

          val result = manager.assignVersion(
            externalIdentifier = newExternalIdentifier,
            ingestId = ingestId,
            ingestDate = Instant.now,
            ingestType = CreateIngestType,
            storageSpace = storageSpace
          )

          result.left.value shouldBe ExternalIdentifiersMismatch(
            stored = storedExternalIdentifier,
            request = newExternalIdentifier
          )
        }
      }
    }

    it("errors if the storage space in the request doesn't match the database") {
      withContext { implicit context =>
        withManager { manager =>
          val ingestId = createIngestID
          val externalIdentifier = createExternalIdentifier

          val storedStorageSpace = createStorageSpace
          val newStorageSpace = createStorageSpace

          manager.assignVersion(
            externalIdentifier = externalIdentifier,
            ingestId = ingestId,
            ingestDate = Instant.now,
            ingestType = CreateIngestType,
            storageSpace = storedStorageSpace
          )

          val result = manager.assignVersion(
            externalIdentifier = externalIdentifier,
            ingestId = ingestId,
            ingestDate = Instant.now,
            ingestType = CreateIngestType,
            storageSpace = newStorageSpace
          )

          result.left.value shouldBe StorageSpaceMismatch(
            stored = storedStorageSpace,
            request = newStorageSpace
          )
        }
      }
    }

    describe("validates the conditions on the version") {
      it("doesn't assign a new version if the ingest date is older") {
        withContext { implicit context =>
          withManager { manager =>
            val externalIdentifier = createExternalIdentifier
            val storageSpace = createStorageSpace

            manager.assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(100),
              ingestType = CreateIngestType,
              storageSpace = storageSpace
            )

            val result = manager.assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(50),
              ingestType = UpdateIngestType,
              storageSpace = storageSpace
            )

            result.left.value shouldBe NewerIngestAlreadyExists(
              stored = Instant.ofEpochSecond(100),
              request = Instant.ofEpochSecond(50)
            )
          }
        }
      }

      it("if ingestType=Create, it doesn't assign anything except v1") {
        val externalIdentifier = createExternalIdentifier
        val storageSpace = createStorageSpace

        withContext { implicit context =>
          withDao { dao =>
            withManager(dao) { manager =>
              manager.assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now(),
                ingestType = CreateIngestType,
                storageSpace = storageSpace
              )

              val result = manager.assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now(),
                ingestType = CreateIngestType,
                storageSpace = storageSpace
              )

              result.left.value shouldBe a[IngestTypeCreateForExistingBag]

              // Check that nothing was written to the database
              dao.lookupLatestVersionFor(
                externalIdentifier = externalIdentifier,
                storageSpace = storageSpace
              ).right.value.version shouldBe BagVersion(1)
            }
          }
        }
      }

      it("if ingestType=Update, it doesn't assign v1") {
        val externalIdentifier = createExternalIdentifier
        val space = createStorageSpace

        withContext { implicit context =>
          withDao { dao: DaoImpl =>
            withManager(dao) { manager =>
              val result = manager.assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now(),
                ingestType = UpdateIngestType,
                storageSpace = space
              )

              result.left.value shouldBe IngestTypeUpdateForNewBag()
            }

            // Check that nothing was written to the database
            dao.lookupLatestVersionFor(
              externalIdentifier = externalIdentifier,
              storageSpace = space
            ).left.value shouldBe a[NoMaximaValueError]
          }
        }
      }
    }

    describe("it handles failures in the underlying dao") {
      it("if lookupExistingVersion has an error") {
        withContext { implicit context =>
          withBrokenLookupExistingVersionDao { dao =>
            withManager(dao) { manager =>
              manager
                .assignVersion(
                  externalIdentifier = createExternalIdentifier,
                  ingestId = createIngestID,
                  ingestDate = Instant.now,
                  ingestType = CreateIngestType,
                  storageSpace = createStorageSpace
                )
                .left
                .value shouldBe an[IngestVersionManagerDaoError]
            }
          }
        }
      }

      it("if lookupLatestVersionFor has an error") {
        withContext { implicit context =>
          withBrokenLookupLatestVersionForDao { dao =>
            withManager(dao) { manager =>
              manager
                .assignVersion(
                  externalIdentifier = createExternalIdentifier,
                  ingestId = createIngestID,
                  ingestDate = Instant.now,
                  ingestType = CreateIngestType,
                  storageSpace = createStorageSpace
                )
                .left
                .value shouldBe an[IngestVersionManagerDaoError]
            }
          }
        }
      }

      it("if storeNewVersion has an error") {
        withContext { implicit context =>
          withBrokenStoreNewVersionDao { dao =>
            withManager(dao) { manager =>
              manager
                .assignVersion(
                  externalIdentifier = createExternalIdentifier,
                  ingestId = createIngestID,
                  ingestDate = Instant.now,
                  ingestType = CreateIngestType,
                  storageSpace = createStorageSpace
                )
                .left
                .value shouldBe an[IngestVersionManagerDaoError]
            }
          }
        }
      }
    }
  }
}
