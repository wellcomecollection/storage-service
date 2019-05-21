package uk.ac.wellcome.platform.archive.common.ingests.monitor
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.ingests.models._

import scala.util.{Failure, Success, Try}

class MemoryIngestTracker extends IngestTracker {
  var ingests: Map[IngestID, Ingest] = Map.empty

  override def get(id: IngestID): Try[Option[Ingest]] = Success(ingests.get(id))

  override def initialise(ingest: Ingest): Try[Ingest] =
    ingests.get(ingest.id) match {
      case Some(_) =>
        Failure(new Throwable(s"Ingest with ID ${ingest.id} already exists"))
      case None =>
        ingests = ingests ++ Map(ingest.id -> ingest)
        Success(ingest)
    }

  override def update(update: IngestUpdate): Try[Ingest] =
    ingests.get(update.id) match {
      case Some(existing: Ingest) =>
        val updatedIngest =
          existing.copy(events = existing.events ++ update.events)

        val newIngest = update match {
          case eventUpdate: IngestEventUpdate =>
            updatedIngest
          case statusUpdate: IngestStatusUpdate =>
            updatedIngest.copy(
              bag = statusUpdate.affectedBag,
              status = statusUpdate.status
            )
          case callbackStatusUpdate: IngestCallbackStatusUpdate =>
            updatedIngest.copy(
              callback = updatedIngest.callback.map { c =>
                c.copy(status = callbackStatusUpdate.callbackStatus)
              }
            )
        }

        ingests = ingests ++ Map(update.id -> newIngest)
        Success(newIngest)
      case None =>
        Failure(
          new Throwable(s"Can't find an existing ingest with ID ${update.id}"))
    }

  override def findByBagId(bagId: BagId): Try[Seq[BagIngest]] = Success(
    ingests.values
      .filter { _.bag.contains(bagId) }
      .zipWithIndex
      .map {
        case (ingest, index) =>
          BagIngest(
            id = ingest.id,
            bagIdIndex = index.toString,
            createdDate = ingest.createdDate
          )
      }
      .toSeq
  )
}
