package uk.ac.wellcome.platform.archive.common.versioning.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.{IngestVersionManagerDao, VersionRecord}
import uk.ac.wellcome.storage.NoMaximaValueError

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
    storageSpace: StorageSpace): Either[NoMaximaValueError, Int] = {
    val matchingVersions =
      records
        .filter { record =>
          record.externalIdentifier == externalIdentifier &&
          record.storageSpace == storageSpace
        }

    if (matchingVersions.isEmpty)
      Left(NoMaximaValueError())
    else
      Right(matchingVersions.maxBy { _.version }.version)
  }

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    records = records :+ record
  }
}
