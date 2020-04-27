package uk.ac.wellcome.platform.archive.indexer.bag

import java.net.URL

import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.display.manifests.DisplayStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer

import scala.concurrent.ExecutionContext

class ManifestIndexer(val client: ElasticClient, val index: Index)(
  implicit
  val ec: ExecutionContext,
  val encoder: Encoder[DisplayStorageManifest]
) extends Indexer[StorageManifest, DisplayStorageManifest] {

  override protected def id(storageManifest: StorageManifest): String =
    storageManifest.idWithVersion

  override protected def toDisplay(
    storageManifest: StorageManifest
  ): DisplayStorageManifest =
    DisplayStorageManifest(
      storageManifest = storageManifest,
      contextUrl = new URL("http://localhost:9200")
    )

  override protected def version(storageManifest: StorageManifest): Long =
    storageManifest.createdDate.toEpochMilli
}
