package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators
import uk.ac.wellcome.platform.archive.common.versioning.memory.MemoryIngestVersionManagerDao

class MemoryIngestVersionManager extends IngestVersionManager {
  val dao = new MemoryIngestVersionManagerDao()
}

class IngestVersionManagerTest
    extends FunSpec
    with Matchers
    with EitherValues
    with ExternalIdentifierGenerators {
  it("assigns version 1 if it hasn't seen this external ID before") {
    val manager = new MemoryIngestVersionManager()

    manager
      .assignVersion(
        externalIdentifier = createExternalIdentifier,
        ingestId = createIngestID,
        ingestDate = Instant.now
      )
      .right
      .value shouldBe 1
  }

  it("assigns increasing versions if it sees newer ingest dates each time") {
    val manager = new MemoryIngestVersionManager()

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

  it("always assigns the same version to a given ingest ID") {
    val manager = new MemoryIngestVersionManager()

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

  it("errors if the external ID in the request doesn't match the database") {
    val manager = new MemoryIngestVersionManager()

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

  it("doesn't assign a new version if the ingest date is older") {
    val manager = new MemoryIngestVersionManager()

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
