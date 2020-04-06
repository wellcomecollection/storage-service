package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import com.sksamuel.elastic4s.requests.indexes.IndexRequest
import com.sksamuel.elastic4s.requests.update.UpdateRequest
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

  def index(manifest: StorageManifest): Future[Unit] = {
    val manifestIndexRequest: IndexRequest =
      indexInto(manifestsIndex)
        .id(manifest.idWithVersion)
        .doc(manifest)

    val fileIndexRequests: Seq[UpdateRequest] =
      manifest.manifest.files.flatMap { file =>
        val bucket = manifest.location.prefix.namespace
        val path = manifest.location.prefix.asLocation(file.path).path

        val upsertRequest =
          update(s"s3://$bucket/$path")
            .in(filesIndex)
            .docAsUpsert(
              "bucket" -> bucket,
              "path" -> path,
              "name" -> file.name,
              "checksum.algorithm" -> manifest.manifest.checksumAlgorithm.toString,
              "checksum.value" -> file.checksum.value,
              "bag.space" -> manifest.space.toString,
              "bag.externalIdentifier" -> manifest.info.externalIdentifier.toString
            )

        val addVersionRequest =
          update(s"s3://$bucket/$path")
            .in(filesIndex)
            .script {
              // TODO: What if the value is already in the array?
              script("ctx._source.bag.versions += version")
                .params(Map("version" -> manifest.version.toString))
            }

        Seq(upsertRequest, addVersionRequest)
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
