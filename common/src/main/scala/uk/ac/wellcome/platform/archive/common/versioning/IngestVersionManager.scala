package uk.ac.wellcome.platform.archive.common.versioning

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID

import scala.util.{Failure, Success}

trait IngestVersionManager {
  val dao: IngestVersionManagerDao

  private def createNewVersionFor(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant
  ): Either[IngestVersionManagerError, Int] =
    dao.lookupLatestVersionFor(externalIdentifier) match {
      case Success(Some(existingRecord)) =>
        if (existingRecord.ingestDate.isBefore(ingestDate))
          storeNewVersion(
            externalIdentifier = externalIdentifier,
            ingestId = ingestId,
            ingestDate = ingestDate,
            newVersion = existingRecord.version + 1
          )
        else
          Left(
            NewerIngestAlreadyExists(
              stored = existingRecord.ingestDate,
              request = ingestDate
            ))

      case Success(None) =>
        storeNewVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate,
          newVersion = 1
        )

      case Failure(err) => Left(IngestVersionManagerDaoError(err))
    }

  private def storeNewVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant,
    newVersion: Int
  ): Either[IngestVersionManagerDaoError, Int] = {
    val newRecord = VersionRecord(
      externalIdentifier = externalIdentifier,
      ingestId = ingestId,
      ingestDate = ingestDate,
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
    ingestDate: Instant
  ): Either[IngestVersionManagerError, Int] =
    dao.lookupExistingVersion(ingestId) match {
      case Success(Some(existingRecord)) =>
        if (existingRecord.externalIdentifier == externalIdentifier)
          Right(existingRecord.version)
        else
          Left(
            ExternalIdentifiersMismatch(
              stored = existingRecord.externalIdentifier,
              request = externalIdentifier
            ))

      case Success(None) =>
        createNewVersionFor(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate
        )

      case Failure(err) => Left(IngestVersionManagerDaoError(err))
    }
}
