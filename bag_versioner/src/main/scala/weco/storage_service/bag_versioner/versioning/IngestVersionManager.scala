package weco.storage_service.bag_versioner.versioning

import java.time.Instant

import weco.storage_service.bagit.models.{BagVersion, ExternalIdentifier}
import weco.storage_service.ingests.models.{
  CreateIngestType,
  IngestID,
  IngestType,
  UpdateIngestType
}
import weco.storage_service.storage.models.StorageSpace
import weco.storage.NoMaximaValueError

import scala.util.{Failure, Success}

trait IngestVersionManager {
  val dao: IngestVersionManagerDao

  def assignVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant,
    ingestType: IngestType,
    storageSpace: StorageSpace
  ): Either[IngestVersionManagerError, BagVersion] = {
    // As in, the previously stored version for this ingest ID.
    val previouslyStoredVersion = dao.lookupExistingVersion(ingestId = ingestId)

    previouslyStoredVersion match {
      case Success(Some(existingVersion)) =>
        verifyExistingVersion(
          existingVersion = existingVersion,
          externalIdentifier = externalIdentifier,
          space = storageSpace
        )

      case Success(None) =>
        createNewVersionFor(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate,
          ingestType = ingestType,
          storageSpace = storageSpace
        )

      case Failure(err) => Left(IngestVersionManagerDaoError(err))
    }
  }

  private def createNewVersionFor(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestDate: Instant,
    ingestType: IngestType,
    storageSpace: StorageSpace
  ): Either[IngestVersionManagerError, BagVersion] = {
    val latestVersion = dao.lookupLatestVersionFor(
      externalIdentifier = externalIdentifier,
      storageSpace = storageSpace
    )

    latestVersion match {
      case Right(existingRecord) if ingestType == CreateIngestType =>
        Left(
          IngestTypeCreateForExistingBag(existingRecord)
        )

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
            )
          )

      case Left(NoMaximaValueError(_)) if ingestType == UpdateIngestType =>
        Left(
          IngestTypeUpdateForNewBag()
        )

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

  /** We always want to assign the same version to a given ingest ID.
    *
    * That way, if a message is double-sent or an ingest gets retried, we
    * won't have multiple versions for the same ingest.
    *
    * This function checks that the external identifier/space on the
    * stored version match the ingest.
    *
    * In theory an ingest is immutable, and the externalIdentifier/space
    * will always be the same.  If they're different, something funky is
    * definitely happening, so we should fail the ingest and flag it for
    * further attention.
    *
    */
  private def verifyExistingVersion(
    existingVersion: VersionRecord,
    externalIdentifier: ExternalIdentifier,
    space: StorageSpace
  ): Either[IngestVersionManagerError, BagVersion] =
    if (existingVersion.externalIdentifier == externalIdentifier &&
        existingVersion.storageSpace == space)
      Right(existingVersion.version)
    else if (existingVersion.externalIdentifier != externalIdentifier)
      Left(
        ExternalIdentifiersMismatch(
          stored = existingVersion.externalIdentifier,
          request = externalIdentifier
        )
      )
    else
      Left(
        StorageSpaceMismatch(
          stored = existingVersion.storageSpace,
          request = space
        )
      )
}
