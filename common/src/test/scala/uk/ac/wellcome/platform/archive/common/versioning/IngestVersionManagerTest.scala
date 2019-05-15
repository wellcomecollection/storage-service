package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators

import scala.util.{Failure, Success, Try}

class MemoryIngestVersionManagerDao extends IngestVersionManagerDao {
  private var versions: List[VersionRecord] = List.empty

  override def lookupExistingVersion(
    ingestID: IngestID): Try[Option[VersionRecord]] = Try {
    versions.find { _.ingestId == ingestID }
  }

  override def lookupLatestVersionFor(
    externalIdentifier: ExternalIdentifier): Try[Option[VersionRecord]] = Try {
    val matchingVersions =
      versions
        .filter { _.externalIdentifier == externalIdentifier }

    if (matchingVersions.isEmpty)
      None
    else
      Some(matchingVersions.maxBy { _.version })
  }

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    versions = versions :+ record
  }
}

class MemoryIngestVersionManager extends IngestVersionManager {
  val dao = new MemoryIngestVersionManagerDao()
}

class IngestVersionManagerTest
    extends FunSpec
    with Matchers
    with ExternalIdentifierGenerators {
  it("assigns version 1 if it hasn't seen this external ID before") {
    val manager = new MemoryIngestVersionManager()

    manager.assignVersion(
      externalIdentifier = createExternalIdentifier,
      ingestId = createIngestID,
      ingestDate = Instant.now
    ) shouldBe Success(1)
  }

  it("assigns increasing versions if it sees newer ingest dates each time") {
    val manager = new MemoryIngestVersionManager()

    val externalIdentifier = createExternalIdentifier

    (1 to 5).map { version =>
      manager.assignVersion(
        externalIdentifier = externalIdentifier,
        ingestId = createIngestID,
        ingestDate = Instant.ofEpochSecond(version)
      ) shouldBe Success(version)
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
          .get

        (idx, ingestId, version)
    }

    assignedVersions.foreach {
      case (idx, ingestId, version) =>
        manager.assignVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = Instant.ofEpochSecond(idx)
        ) shouldBe Success(version)
    }
  }

  it("errors if the external ID in the request doesn't match the database") {
    val manager = new MemoryIngestVersionManager()

    val ingestId = createIngestID

    manager.assignVersion(
      externalIdentifier = createExternalIdentifier,
      ingestId = ingestId,
      ingestDate = Instant.now
    )

    val result = manager.assignVersion(
      externalIdentifier = createExternalIdentifier,
      ingestId = ingestId,
      ingestDate = Instant.now
    )

    result shouldBe a[Failure[_]]
    result.failed.get.getMessage should startWith(
      "External identifiers don't match:")
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

    result shouldBe a[Failure[_]]
    result.failed.get.getMessage should startWith(
      "Latest version has a newer ingest date:")
  }
}
