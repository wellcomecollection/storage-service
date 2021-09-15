package weco.storage_service.bag_versioner.versioning

import java.time.Instant

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import weco.storage_service.ingests.models.{CreateIngestType, UpdateIngestType}
import weco.storage.NoMaximaValueError

trait IngestVersionManagerTestCases[DaoImpl <: IngestVersionManagerDao, Context]
    extends AnyFunSpec
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

  def withManagerImpl[R](
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
        withManagerImpl { manager =>
          manager
            .assignVersion(
              externalIdentifier = createExternalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              space = createStorageSpace
            )
            .value shouldBe BagVersion(1)
        }
      }
    }

    it("assigns increasing versions if it sees newer ingest dates each time") {
      val externalIdentifier = createExternalIdentifier
      val space = createStorageSpace

      withContext { implicit context =>
        withManagerImpl { manager =>
          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(1),
              ingestType = CreateIngestType,
              space = space
            )
            .value shouldBe BagVersion(1)

          (2 to 5).map { version =>
            manager
              .assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.ofEpochSecond(version),
                ingestType = UpdateIngestType,
                space = space
              )
              .value shouldBe BagVersion(version)
          }
        }
      }
    }

    it("always assigns the same version to a given ingest ID") {
      val externalIdentifier = createExternalIdentifier
      val space = createStorageSpace

      withContext { implicit context =>
        withManagerImpl { manager =>
          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(0),
              ingestType = CreateIngestType,
              space = space
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
                  space = space
                )
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
                  space = space
                )
                .value shouldBe version
          }
        }
      }
    }

    it(
      "assigns independent versions to different external IDs in the same space"
    ) {
      withContext { implicit context =>
        withManagerImpl { manager =>
          val space = createStorageSpace

          manager
            .assignVersion(
              externalIdentifier = createExternalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              space = space
            )
            .value shouldBe BagVersion(1)

          manager
            .assignVersion(
              externalIdentifier = createExternalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              space = space
            )
            .value shouldBe BagVersion(1)
        }
      }
    }

    it(
      "assigns independent versions to the same external ID in different spaces"
    ) {
      withContext { implicit context =>
        withManagerImpl { manager =>
          val externalIdentifier = createExternalIdentifier

          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              space = createStorageSpace
            )
            .value shouldBe BagVersion(1)

          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              space = createStorageSpace
            )
            .value shouldBe BagVersion(1)
        }
      }
    }

    it("errors if the external ID in the request doesn't match the database") {
      withContext { implicit context =>
        withManagerImpl { manager =>
          val ingestId = createIngestID
          val space = createStorageSpace

          val storedExternalIdentifier = createExternalIdentifier

          manager.assignVersion(
            externalIdentifier = storedExternalIdentifier,
            ingestId = ingestId,
            ingestDate = Instant.now,
            ingestType = CreateIngestType,
            space = space
          )

          val newExternalIdentifier = createExternalIdentifier

          val result = manager.assignVersion(
            externalIdentifier = newExternalIdentifier,
            ingestId = ingestId,
            ingestDate = Instant.now,
            ingestType = CreateIngestType,
            space = space
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
        withManagerImpl { manager =>
          val ingestId = createIngestID
          val externalIdentifier = createExternalIdentifier

          val storedSpace = createStorageSpace
          val newSpace = createStorageSpace

          manager.assignVersion(
            externalIdentifier = externalIdentifier,
            ingestId = ingestId,
            ingestDate = Instant.now,
            ingestType = CreateIngestType,
            space = storedSpace
          )

          val result = manager.assignVersion(
            externalIdentifier = externalIdentifier,
            ingestId = ingestId,
            ingestDate = Instant.now,
            ingestType = CreateIngestType,
            space = newSpace
          )

          result.left.value shouldBe StorageSpaceMismatch(
            stored = storedSpace,
            request = newSpace
          )
        }
      }
    }

    describe("validates the conditions on the version") {
      it("doesn't assign a new version if the ingest date is older") {
        withContext { implicit context =>
          withManagerImpl { manager =>
            val externalIdentifier = createExternalIdentifier
            val space = createStorageSpace

            manager.assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(100),
              ingestType = CreateIngestType,
              space = space
            )

            val result = manager.assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(50),
              ingestType = UpdateIngestType,
              space = space
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
        val space = createStorageSpace

        withContext { implicit context =>
          withDao { dao =>
            withManager(dao) { manager =>
              manager.assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now(),
                ingestType = CreateIngestType,
                space = space
              )

              val result = manager.assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now(),
                ingestType = CreateIngestType,
                space = space
              )

              result.left.value shouldBe a[IngestTypeCreateForExistingBag]

              // Check that nothing was written to the database
              dao
                .lookupLatestVersionFor(
                  externalIdentifier = externalIdentifier,
                  space = space
                )
                .value
                .version shouldBe BagVersion(1)
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
                space = space
              )

              result.left.value shouldBe IngestTypeUpdateForNewBag()
            }

            // Check that nothing was written to the database
            dao
              .lookupLatestVersionFor(
                externalIdentifier = externalIdentifier,
                space = space
              )
              .left
              .value shouldBe a[NoMaximaValueError]
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
                  space = createStorageSpace
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
                  space = createStorageSpace
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
                  space = createStorageSpace
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
