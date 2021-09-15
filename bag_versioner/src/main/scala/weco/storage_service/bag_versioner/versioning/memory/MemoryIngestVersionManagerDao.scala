package weco.storage_service.bag_versioner.versioning.memory

import weco.storage_service.bagit.models.ExternalIdentifier
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models.StorageSpace
import weco.storage_service.bag_versioner.versioning.{
  IngestVersionManagerDao,
  VersionRecord
}
import weco.storage.{MaximaError, NoMaximaValueError}

import scala.util.{Failure, Success, Try}

class MemoryIngestVersionManagerDao() extends IngestVersionManagerDao {
  var records: Seq[VersionRecord] = Seq.empty

  override def lookupExistingVersion(
    ingestID: IngestID
  ): Try[Option[VersionRecord]] =
    records.filter { _.ingestId == ingestID } match {
      case Seq(record) => Success(Some(record))
      case Nil         => Success(None)
      case _           => Failure(new Throwable("Too many matching entries!"))
    }

  override def lookupLatestVersionFor(
    externalIdentifier: ExternalIdentifier,
    space: StorageSpace
  ): Either[MaximaError, VersionRecord] = {
    val matchingVersions =
      records
        .filter { record =>
          record.externalIdentifier == externalIdentifier &&
          record.storageSpace == space
        }

    if (matchingVersions.isEmpty) {
      val error = new Error(
        s"No versions found for bag $space/$externalIdentifier"
      )
      Left(NoMaximaValueError(error))
    } else
      Right(matchingVersions.maxBy { _.version.underlying })
  }

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Try {
    records = records :+ record
  }
}
