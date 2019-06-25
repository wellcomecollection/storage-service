package uk.ac.wellcome.platform.archive.common.versioning.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManagerDao, VersionRecord}

import scala.util.{Failure, Success, Try}

class MemoryIngestVersionManagerDao() extends IngestVersionManagerDao {
  var records: Seq[VersionRecord] = Seq.empty

  override def lookupExistingVersion(
    ingestID: IngestID): Try[Option[VersionRecord]] =
    records.filter { _.ingestId == ingestID } match {
      case Seq(record) => Success(Some(record))
      case Nil         => Success(None)
      case _           => Failure(new Throwable("Too many matching entries!"))
    }

  override def lookupLatestVersionFor(
    externalIdentifier: ExternalIdentifier,
    storageSpace: StorageSpace): Try[Option[VersionRecord]] = Try {
    val matchingVersions =
      records
        .filter { _.externalIdentifier == externalIdentifier }

    if (matchingVersions.isEmpty)
      None
    else
      Some(matchingVersions.maxBy { _.version })
  }

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    records = records :+ record
  }
}
