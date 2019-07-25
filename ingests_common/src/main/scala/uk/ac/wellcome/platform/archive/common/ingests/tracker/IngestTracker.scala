package uk.ac.wellcome.platform.archive.common.ingests.tracker

import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.ingests.services.{
  CallbackStatusGoingBackwardsException,
  IngestStates,
  IngestStatusGoingBackwardsException,
  NoCallbackException
}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.VersionedStore

import scala.util.{Failure, Success, Try}

trait IngestTracker {
  val underlying: VersionedStore[IngestID, Int, Ingest]

  type Result =
    Either[IngestTrackerError, Identified[Version[IngestID, Int], Ingest]]

  def init(ingest: Ingest): Result =
    underlying.init(ingest.id)(ingest) match {
      case Right(value) => Right(value)
      case Left(err: VersionAlreadyExistsError) =>
        Left(IngestAlreadyExistsError(err))
      case Left(err) => Left(IngestTrackerStoreError(err))
    }

  def get(id: IngestID): Result =
    underlying.getLatest(id) match {
      case Right(value)             => Right(value)
      case Left(err: NotFoundError) => Left(IngestDoesNotExistError(err))
      case Left(err)                => Left(IngestTrackerStoreError(err))
    }

  def update(update: IngestUpdate): Result = {
    // We have to do this slightly icky callback because there's no
    // way to tell VersionedStore.update() to skip the update inside
    // the callback.
    val updateCallback =
      (ingest: Ingest) => IngestStates.applyUpdate(ingest, update).get

    Try { underlying.update(update.id)(updateCallback) } match {
      case Success(Right(result)) => Right(result)

      case Success(Left(err: UpdateNoSourceError)) =>
        Left(UpdateNonExistentIngestError(err))

      case Failure(err: IngestStatusGoingBackwardsException) =>
        Left(IngestStatusGoingBackwards(err.existing, err.update))

      case Failure(err: CallbackStatusGoingBackwardsException) =>
        Left(IngestCallbackStatusGoingBackwards(err.existing, err.update))

      case Failure(_: NoCallbackException) =>
        Left(NoCallbackOnIngest())

      case Success(Left(err)) => Left(IngestTrackerStoreError(err))

      case Failure(err) => Left(IngestTrackerUpdateError(err))
    }
  }

  /** Given a bag ID, find the ingests that it was involved in.
    *
    * This is intended to meet a particular use case for DLCS during migration and not as part of the
    * public/documented API.  Consider either removing this functionality or enhancing it to be fully
    * featured if a use case arises after migration.
    *
    */
  def listByBagId(bagId: BagId): Either[IngestTrackerError, Seq[Ingest]]
}
