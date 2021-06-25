package weco.storage_service.indexer.bags

import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe.Encoder
import weco.storage_service.storage.models.StorageManifest
import weco.storage_service.indexer.bags.models.IndexedStorageManifest
import weco.storage_service.indexer.Indexer

import scala.concurrent.ExecutionContext

class BagIndexer(val client: ElasticClient, val index: Index)(
  implicit
  val ec: ExecutionContext,
  val encoder: Encoder[IndexedStorageManifest]
) extends Indexer[StorageManifest, IndexedStorageManifest] {

  override def id(storageManifest: StorageManifest): String =
    storageManifest.id.toString

  override protected def toDisplay(
    storageManifest: StorageManifest
  ): IndexedStorageManifest =
    IndexedStorageManifest(storageManifest)

  override protected def version(storageManifest: StorageManifest): Long =
    storageManifest.version.underlying
}
