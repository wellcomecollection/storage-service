package weco.storage_service.bag_versioner.versioning

import java.time.Instant
import java.util.UUID

import cats.{Id, Monad, MonadError}
import weco.storage_service.bagit.models.{
  BagId,
  BagVersion,
  ExternalIdentifier
}
import weco.storage_service.ingests.models.{
  IngestID,
  IngestType
}
import weco.storage_service.storage.models.StorageSpace
import weco.storage.locking.{
  FailedLockingServiceOp,
  LockDao,
  LockingService
}

class VersionPicker(
  lockingService: LockingService[
    Either[IngestVersionManagerError, BagVersion],
    Id,
    LockDao[String, UUID]
  ],
  ingestVersionManager: IngestVersionManager
) {
  def chooseVersion(
    externalIdentifier: ExternalIdentifier,
    ingestId: IngestID,
    ingestType: IngestType,
    ingestDate: Instant,
    storageSpace: StorageSpace
  ): Either[VersionPickerError, BagVersion] = {
    val bagId = BagId(
      space = storageSpace,
      externalIdentifier = externalIdentifier
    )

    val locks = Set(s"ingest:$ingestId", s"bag:$bagId")

    val assignedVersion: Id[lockingService.Process] = lockingService
      .withLocks(locks) {
        ingestVersionManager.assignVersion(
          externalIdentifier = externalIdentifier,
          ingestId = ingestId,
          ingestDate = ingestDate,
          ingestType = ingestType,
          storageSpace = storageSpace
        )
      }

    // This is a double Either: the outer Either defines the result of the locking
    // service operation, the inner Either is the version assignment.
    assignedVersion match {
      case Right(Right(version)) =>
        Right(version)

      case Right(Left(ingestVersionManagerError)) =>
        Left(UnableToAssignVersion(ingestVersionManagerError))

      case Left(err: FailedLockingServiceOp) =>
        Left(FailedToGetLock(err))
    }
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
