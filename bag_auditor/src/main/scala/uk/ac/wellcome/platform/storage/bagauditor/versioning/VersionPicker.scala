package uk.ac.wellcome.platform.storage.bagauditor.versioning

import java.time.Instant
import java.util.UUID

import cats.{Id, Monad, MonadError}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  IngestID,
  IngestType,
  UpdateIngestType
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.dynamo.DynamoID
import uk.ac.wellcome.platform.archive.common.versioning.{
  IngestVersionManager,
  IngestVersionManagerError
}
import uk.ac.wellcome.platform.storage.bagauditor.models._
import uk.ac.wellcome.storage.locking.{FailedProcess, LockDao, LockingService}

class VersionPicker(
  lockingService: LockingService[Either[IngestVersionManagerError, Int],
                                 Id,
                                 LockDao[String, UUID]],
  ingestVersionManager: IngestVersionManager
) {
  def chooseVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestType: IngestType,
    ingestDate: Instant,
    storageSpace: StorageSpace
  ): Either[VersionPickerError, Int] = {
    val assignedVersion: Id[lockingService.Process] = lockingService
      .withLocks(
        Set(
          s"ingest:$ingestId",
          s"external:${DynamoID.createId(storageSpace, externalIdentifier)}")) {
        ingestVersionManager.assignVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate,
          storageSpace = storageSpace
        )
      }

    // This is a double Either: the outer Either defines the result of the locking
    // service operation, the inner Either is the version assignment.
    assignedVersion match {
      case Right(Right(version)) =>
        checkVersionIsAllowed(ingestType, assignedVersion = version)

      case Right(Left(ingestVersionManagerError)) =>
        Left(UnableToAssignVersion(ingestVersionManagerError))

      case Left(FailedProcess(_, err)) => Left(InternalVersionPickerError(err))
      case Left(err) =>
        Left(InternalVersionPickerError(new Throwable(s"Locking error: $err")))
    }
  }

  private def checkVersionIsAllowed(
    ingestType: IngestType,
    assignedVersion: Int): Either[VersionPickerError, Int] =
    if (ingestType == CreateIngestType && assignedVersion > 1) {
      Left(IngestTypeCreateForExistingBag())
    } else if (ingestType == UpdateIngestType && assignedVersion == 1) {
      Left(IngestTypeUpdateForNewBag())
    } else {
      Right(assignedVersion)
    }

  // Annoyingly, cats doesn't provide an Implicit for MonadError[Id, Throwable],
  // so we have to implement one ourselves.
  implicit def idMonad(implicit I: Monad[Id]): MonadError[Id, Throwable] =
    new MonadError[Id, Throwable] {
      override def raiseError[A](e: Throwable): Id[A] = throw e

      override def handleErrorWith[A](fa: Id[A])(f: Throwable => Id[A]): Id[A] =
        try {
          fa
        } catch {
          case t: Throwable => f(t)
        }

      override def pure[A](x: A): Id[A] = x

      override def flatMap[A, B](fa: Id[A])(f: A => Id[B]): Id[B] =
        I.flatMap(fa)(f)

      override def tailRecM[A, B](a: A)(f: A => Id[Either[A, B]]): Id[B] =
        I.tailRecM(a)(f)
    }
}
