package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.indexes.IndexResponse
import com.sksamuel.elastic4s.{ElasticClient, Index, Indexable, Response}
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

import scala.concurrent.{ExecutionContext, Future}

class BagIndexer(
  elasticClient: ElasticClient,
  manifestsIndex: Index,
  filesIndex: Index
)(
  implicit ec: ExecutionContext
) extends Logging {
  implicit object IdentifiedWorkIndexable extends Indexable[StorageManifest] {
    override def json(t: StorageManifest): String =
      toJson(t).get
  }

  def index(manifest: StorageManifest): Future[Unit] =
    elasticClient
      .execute {
        indexInto(manifestsIndex)
          .id(manifest.idWithVersion)
          .doc(manifest)
      }
      .flatMap { response: Response[IndexResponse] =>
        if (response.isError) {
          warn(s"Error from Elasticsearch: $response")
          Future.failed(response.error.asException)
        } else {
          debug(s"Index response = $response")
          Future.successful(())
        }
      }
}
