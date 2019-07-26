package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.NoMaximaValueError

import scala.util.{Failure, Success}

trait IngestVersionManager {
  val dao: IngestVersionManagerDao

  private def createNewVersionFor(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant,
    storageSpace: StorageSpace
  ): Either[IngestVersionManagerError, BagVersion] =
    dao.lookupLatestVersionFor(externalIdentifier, storageSpace) match {
      case Right(existingRecord) =>
        if (existingRecord.ingestDate.isBefore(ingestDate))
          storeNewVersion(
            externalIdentifier = externalIdentifier,
            ingestId = ingestId,
            ingestDate = ingestDate,
            storageSpace = storageSpace,
            newVersion = existingRecord.version.increment
          )
        else
          Left(
            NewerIngestAlreadyExists(
              stored = existingRecord.ingestDate,
              request = ingestDate
            ))

      case Left(NoMaximaValueError(_)) =>
        storeNewVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate,
          storageSpace = storageSpace,
          newVersion = BagVersion(1)
        )

      // TODO: Can we preserve the StorageError here?
      case Left(err) => Left(IngestVersionManagerDaoError(err.e))
    }

  private def storeNewVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant,
    storageSpace: StorageSpace,
    newVersion: BagVersion
  ): Either[IngestVersionManagerDaoError, BagVersion] = {
    val newRecord = VersionRecord(
      externalIdentifier = externalIdentifier,
      ingestId = ingestId,
      ingestDate = ingestDate,
      storageSpace = storageSpace,
      version = newVersion
    )

    dao.storeNewVersion(newRecord) match {
      case Success(_)   => Right(newVersion)
      case Failure(err) => Left(IngestVersionManagerDaoError(err))
    }
  }

  def assignVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant,
    storageSpace: StorageSpace
  ): Either[IngestVersionManagerError, BagVersion] =
    dao.lookupExistingVersion(ingestId) match {
      case Success(Some(existingRecord)) =>
        if (existingRecord.externalIdentifier == externalIdentifier &&
            existingRecord.storageSpace == storageSpace)
          Right(existingRecord.version)
        else if (existingRecord.externalIdentifier != externalIdentifier)
          Left(
            ExternalIdentifiersMismatch(
              stored = existingRecord.externalIdentifier,
              request = externalIdentifier
            ))
        else
          Left(
            StorageSpaceMismatch(
              stored = existingRecord.storageSpace,
              request = storageSpace
            ))

      case Success(None) =>
        createNewVersionFor(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate,
          storageSpace = storageSpace
        )

      case Failure(err) => Left(IngestVersionManagerDaoError(err))
    }
}
