package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.bulk.{
  BulkCompatibleRequest,
  BulkResponse
}
import com.sksamuel.elastic4s.requests.indexes.IndexRequest
import com.sksamuel.elastic4s.{ElasticClient, Index, Indexable, Response}
import grizzled.slf4j.Logging
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_indexer.models.IndexedFile
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

import scala.concurrent.{ExecutionContext, Future}

class BagIndexer(
  elasticClient: ElasticClient,
  manifestsIndex: Index,
  filesIndex: Index
)(
  implicit ec: ExecutionContext
) extends Logging {
  implicit object StorageManifestIndexable extends Indexable[StorageManifest] {
    override def json(manifest: StorageManifest): String =
      toJson(manifest).get
  }

  implicit object IndexedFileIndexable extends Indexable[IndexedFile] {
    override def json(indexedFile: IndexedFile): String =
      toJson(indexedFile).get
  }

  def index(manifest: StorageManifest): Future[Unit] = {
    val manifestIndexRequest: IndexRequest =
      indexInto(manifestsIndex)
        .id(manifest.idWithVersion)
        .doc(manifest)

    val fileIndexRequests: Seq[BulkCompatibleRequest] =
      manifest.manifest.files.map { file =>
        val indexedFile = IndexedFile(manifest, file)

        if (file.path.startsWith(s"${manifest.version.toString}")) {
          indexInto(filesIndex)
            .id(indexedFile.id)
            .doc(indexedFile)
        } else {
          update(indexedFile.id)
            .in(filesIndex)
            .script {
              s"""
                 |if (!ctx._source.bag.versions.contains('${manifest.version.toString}')) {
                 |  ctx._source.bag.versions.add('${manifest.version.toString}');
                 |}
               """.stripMargin
            }
        }
      }

    elasticClient
      .execute {
        bulk {
          manifestIndexRequest +: fileIndexRequests
        }
      }
      .flatMap { response: Response[BulkResponse] =>
        println(response.result.errors)
        if (response.isError) {
          warn(s"Error from Elasticsearch: $response")
          Future.failed(response.error.asException)
        } else if (response.result.errors) {
          warn(s"Bulk response completed with errors: $response")
          Future.failed(new Throwable("Error during bulk indexing"))
        } else {
          debug(s"Index response = $response")
          Future.successful(())
        }
      }
  }
}
