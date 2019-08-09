package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}

trait IngestVersionManagerTestCases[DaoImpl, Context]
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

  describe("behaves as an ingest version manager") {
    it("assigns version 1 if it hasn't seen this external ID before") {
      withContext { implicit context =>
        withDao { dao =>
          withManager(dao) { manager =>
            manager
              .assignVersion(
                externalIdentifier = createExternalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now,
                storageSpace = createStorageSpace
              )
              .right
              .value shouldBe BagVersion(1)
          }
        }
      }
    }

    it("assigns increasing versions if it sees newer ingest dates each time") {
      withContext { implicit context =>
        withDao { dao =>
          withManager(dao) { manager =>
            val externalIdentifier = createExternalIdentifier
            val storageSpace = createStorageSpace

            (1 to 5).map { version =>
              manager
                .assignVersion(
                  externalIdentifier = externalIdentifier,
                  ingestId = createIngestID,
                  ingestDate = Instant.ofEpochSecond(version),
                  storageSpace = storageSpace
                )
                .right
                .value shouldBe BagVersion(version)
            }
          }
        }
      }
    }

    it("always assigns the same version to a given ingest ID") {
      withContext { implicit context =>
        withDao { dao =>
          withManager(dao) { manager =>
            val externalIdentifier = createExternalIdentifier
            val storageSpace = createStorageSpace

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
                    storageSpace = storageSpace
                  )
                  .right
                  .value shouldBe version
            }
          }
        }
      }
    }

    it(
      "assigns independent versions to different external IDs in the same space"
    ) {
      withContext { implicit context =>
        withDao { dao =>
          withManager(dao) { manager =>
            val storageSpace = createStorageSpace

            manager
              .assignVersion(
                externalIdentifier = createExternalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now,
                storageSpace = storageSpace
              )
              .right
              .value shouldBe BagVersion(1)

            manager
              .assignVersion(
                externalIdentifier = createExternalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now,
                storageSpace = storageSpace
              )
              .right
              .value shouldBe BagVersion(1)
          }
        }
      }
    }

    it(
      "assigns independent versions to the same external ID in different spaces"
    ) {
      withContext { implicit context =>
        withDao { dao =>
          withManager(dao) { manager =>
            val externalIdentifier = createExternalIdentifier

            manager
              .assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now,
                storageSpace = createStorageSpace
              )
              .right
              .value shouldBe BagVersion(1)

            manager
              .assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = createIngestID,
                ingestDate = Instant.now,
                storageSpace = createStorageSpace
              )
              .right
              .value shouldBe BagVersion(1)
          }
        }
      }
    }

    it("errors if the external ID in the request doesn't match the database") {
      withContext { implicit context =>
        withDao { dao =>
          withManager(dao) { manager =>
            val ingestId = createIngestID
            val storageSpace = createStorageSpace

            val storedExternalIdentifier = createExternalIdentifier

            manager.assignVersion(
              externalIdentifier = storedExternalIdentifier,
              ingestId = ingestId,
              ingestDate = Instant.now,
              storageSpace = storageSpace
            )

            val newExternalIdentifier = createExternalIdentifier

            val result = manager.assignVersion(
              externalIdentifier = newExternalIdentifier,
              ingestId = ingestId,
              ingestDate = Instant.now,
              storageSpace = storageSpace
            )

            result.left.value shouldBe ExternalIdentifiersMismatch(
              stored = storedExternalIdentifier,
              request = newExternalIdentifier
            )
          }
        }
      }
    }

    it("errors if the storage space in the request doesn't match the database") {
      withContext { implicit context =>
        withDao { dao =>
          withManager(dao) { manager =>
            val ingestId = createIngestID
            val externalIdentifier = createExternalIdentifier

            val storedStorageSpace = createStorageSpace
            val newStorageSpace = createStorageSpace

            manager.assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = ingestId,
              ingestDate = Instant.now,
              storageSpace = storedStorageSpace
            )

            val result = manager.assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = ingestId,
              ingestDate = Instant.now,
              storageSpace = newStorageSpace
            )

            result.left.value shouldBe StorageSpaceMismatch(
              stored = storedStorageSpace,
              request = newStorageSpace
            )
          }
        }
      }
    }

    it("doesn't assign a new version if the ingest date is older") {
      withContext { implicit context =>
        withDao { dao =>
          withManager(dao) { manager =>
            val externalIdentifier = createExternalIdentifier
            val storageSpace = createStorageSpace

            manager.assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(100),
              storageSpace = storageSpace
            )

            val result = manager.assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(50),
              storageSpace = storageSpace
            )

            result.left.value shouldBe NewerIngestAlreadyExists(
              stored = Instant.ofEpochSecond(100),
              request = Instant.ofEpochSecond(50)
            )
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
