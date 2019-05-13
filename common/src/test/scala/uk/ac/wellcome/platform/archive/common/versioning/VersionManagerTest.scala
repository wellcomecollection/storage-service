package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.ExternalIdentifierGenerators

import scala.util.{Success, Try}

class MemoryVersionManager extends VersionManager {
  private var versions: List[VersionRecord] = List.empty

  override protected def lookupExistingVersion(ingestID: IngestID): Try[Option[VersionRecord]] = Try {
    versions.find { _.ingestID == ingestID }
  }

  override protected def lookupLatestVersionFor(externalIdentifier: ExternalIdentifier): Try[Option[VersionRecord]] = Try {
    val matchingVersions =
      versions
        .filter { _.externalIdentifier == externalIdentifier }

    if (matchingVersions.isEmpty)
      None
    else
      Some(matchingVersions.maxBy { _.version })
  }

  override protected def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    versions = versions :+ record
  }
}

class VersionManagerTest extends FunSpec with Matchers with ExternalIdentifierGenerators {
  it("assigns version 1 if it hasn't seen this external ID before") {
    val manager = new MemoryVersionManager()

    manager.assignVersion(
      externalIdentifier = createExternalIdentifier,
      ingestId = createIngestID,
      ingestDate = Instant.now
    ) shouldBe Success(1)
  }
}
