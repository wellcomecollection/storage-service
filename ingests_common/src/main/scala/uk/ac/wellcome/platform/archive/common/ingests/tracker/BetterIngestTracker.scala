package uk.ac.wellcome.platform.archive.common.ingests.tracker
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.storage.store.VersionedStore

trait BetterIngestTracker[StoreImpl <: VersionedStore[IngestID, Int, Ingest]] {
  val underlying: StoreImpl

  def get(id: IngestID): underlying.ReadEither =
    underlying.getLatest(id)

  def init(ingest: Ingest): underlying.WriteEither =
    underlying.init(ingest.id)(ingest)

  def update(update: IngestUpdate): underlying.UpdateEither = {
    update match {
      case _: IngestEventUpdate =>
        underlying.update(update.id) { ingest =>
          ingest.copy(
            events = ingest.events ++ update.events
          )
        }

      case statusUpdate: IngestStatusUpdate =>
        underlying.update(update.id) { ingest =>
          // TODO: What if the update conflicts with the ingest?
          ingest.copy(
            bag = statusUpdate.affectedBag,
            status = statusUpdate.status,
            events = ingest.events ++ update.events
          )
        }

      case callbackStatusUpdate: IngestCallbackStatusUpdate =>
        underlying.update(update.id) { ingest =>
          // TODO: What if there's no status?
          ingest.copy(
            callback = ingest.callback.map { cb =>
              cb.copy(status = callbackStatusUpdate.callbackStatus)
            }
          )
        }
    }
  }
}
