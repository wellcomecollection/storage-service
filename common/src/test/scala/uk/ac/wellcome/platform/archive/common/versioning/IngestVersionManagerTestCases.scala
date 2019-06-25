package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators

trait IngestVersionManagerTestCases[Context] extends FunSpec with Matchers with EitherValues with ExternalIdentifierGenerators {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withManager[R](testWith: TestWith[IngestVersionManager, R])(implicit context: Context): R

  // TODO: Add a test for a failure in the underlying dao

  it("assigns version 1 if it hasn't seen this external ID before") {
    withContext { implicit context =>
      withManager { manager =>
        manager
          .assignVersion(
            externalIdentifier = createExternalIdentifier,
            ingestId = createIngestID,
            ingestDate = Instant.now
          )
          .right
          .value shouldBe 1
      }
    }
  }

  it("assigns increasing versions if it sees newer ingest dates each time") {
    withContext { implicit context =>
      withManager { manager =>
        val externalIdentifier = createExternalIdentifier

        (1 to 5).map { version =>
          manager
            .assignVersion(
              externalIdentifier = externalIdentifier,
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(version)
            )
            .right
            .value shouldBe version
        }
      }
    }
  }

  it("always assigns the same version to a given ingest ID") {
    withContext { implicit context =>
      withManager { manager =>
        val externalIdentifier = createExternalIdentifier

        val ingestIds = (1 to 5).map { idx =>
          (idx, createIngestID)
        }

        val assignedVersions = ingestIds.map {
          case (idx, ingestId) =>
            val version = manager
              .assignVersion(
                externalIdentifier = externalIdentifier,
                ingestId = ingestId,
                ingestDate = Instant.ofEpochSecond(idx)
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
                ingestDate = Instant.ofEpochSecond(idx)
              )
              .right
              .value shouldBe version
        }
      }
    }
  }

  it("errors if the external ID in the request doesn't match the database") {
    withContext { implicit context =>
      withManager { manager =>
        val ingestId = createIngestID

        val storedExternalIdentifier = createExternalIdentifier

        manager.assignVersion(
          externalIdentifier = storedExternalIdentifier,
          ingestId = ingestId,
          ingestDate = Instant.now
        )

        val newExternalIdentifier = createExternalIdentifier

        val result = manager.assignVersion(
          externalIdentifier = newExternalIdentifier,
          ingestId = ingestId,
          ingestDate = Instant.now
        )

        result.left.value shouldBe ExternalIdentifiersMismatch(
          stored = storedExternalIdentifier,
          request = newExternalIdentifier
        )
      }
    }
  }

  it("doesn't assign a new version if the ingest date is older") {
    withContext { implicit context =>
      withManager { manager =>
        val externalIdentifier = createExternalIdentifier

        manager.assignVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestDate = Instant.ofEpochSecond(100)
        )

        val result = manager.assignVersion(
          externalIdentifier = externalIdentifier,
          ingestId = createIngestID,
          ingestDate = Instant.ofEpochSecond(50)
        )

        result.left.value shouldBe NewerIngestAlreadyExists(
          stored = Instant.ofEpochSecond(100),
          request = Instant.ofEpochSecond(50)
        )
      }
    }
  }
}
