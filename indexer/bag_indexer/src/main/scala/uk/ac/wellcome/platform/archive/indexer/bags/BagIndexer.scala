package uk.ac.wellcome.platform.archive.indexer.bags

import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe.Encoder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer

import scala.concurrent.ExecutionContext

class BagIndexer(val client: ElasticClient, val index: Index)(
  implicit
  val ec: ExecutionContext,
  val encoder: Encoder[IndexedStorageManifest]
) extends Indexer[StorageManifest, IndexedStorageManifest] {

  override protected def id(storageManifest: StorageManifest): String =
    storageManifest.id.toString

  override protected def toDisplay(
    storageManifest: StorageManifest
  ): IndexedStorageManifest =
    IndexedStorageManifest(storageManifest)

  override protected def version(storageManifest: StorageManifest): Long =
    storageManifest.version.underlying
}
