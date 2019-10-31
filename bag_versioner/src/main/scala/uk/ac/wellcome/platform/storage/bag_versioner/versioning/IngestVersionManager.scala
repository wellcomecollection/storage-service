package uk.ac.wellcome.platform.storage.bag_versioner.versioning

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
            )
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
          storageSpace = storageSpace
        )

      case Failure(err) => Left(IngestVersionManagerDaoError(err))
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
