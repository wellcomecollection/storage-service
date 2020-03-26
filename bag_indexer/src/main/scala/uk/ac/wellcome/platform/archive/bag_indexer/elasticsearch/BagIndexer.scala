package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.{ElasticClient, Index}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

import scala.concurrent.Future

class BagIndexer(
  elasticClient: ElasticClient,
  manifestsIndex: Index,
  filesIndex: Index
) {
  def index(manifest: StorageManifest): Future[Unit] = {
    Future.failed(new RuntimeException("BOOM!"))
  }
}
