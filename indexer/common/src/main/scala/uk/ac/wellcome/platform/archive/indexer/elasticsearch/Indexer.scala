package uk.ac.wellcome.platform.archive.indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.bulk.{BulkResponse, BulkResponseItem}
import com.sksamuel.elastic4s.requests.common.VersionType.ExternalGte
import com.sksamuel.elastic4s.{ElasticClient, Index, Indexable, Response}
import grizzled.slf4j.Logging
import io.circe.Encoder
import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.{ExecutionContext, Future}

trait Indexer[Document, DisplayDocument] extends Logging {
  val client: ElasticClient
  val index: Index

  implicit val ec: ExecutionContext
  implicit val encoder: Encoder[DisplayDocument]

  implicit val indexable: Indexable[DisplayDocument] =
    (displayDocument: DisplayDocument) => toJson(displayDocument).get

  def id(doc: Document): String
  protected def toDisplay(doc: Document): DisplayDocument
  protected def version(doc: Document): Long

  final def index(
    documents: Seq[Document]
  ): Future[Either[Seq[Document], Seq[Document]]] = {
    debug(s"Indexing documents: ${documents.map { id }.mkString(", ")}")

    val inserts = documents.map { doc =>
      indexInto(index)
        .id { id(doc) }
        .version(version(doc))
        .versionType(ExternalGte)
        .doc { toDisplay(doc) }
    }

    client
      .execute { bulk(inserts) }
      .map { response: Response[BulkResponse] =>
        if (response.isError) {
          error(s"Error from Elasticsearch: $response")
          Left(documents)
        } else {
          val actualFailures =
            response.result.failures
              .filterNot { isVersionConflictException }

          if (actualFailures.isEmpty) {
            Right(documents)
          } else {
            val failedIds = actualFailures.map { failure =>
              error(s"Error indexing ${failure.id}: ${failure.error}")
              failure.id
            }.toSet

            val failedDocuments = documents.filter { doc =>
              failedIds.contains(id(doc))
            }

            Left(failedDocuments)
          }
        }
      }
  }

  final def index(document: Document): Future[Either[Document, Document]] =
    index(Seq(document))
      .map {
        case Right(Seq(doc)) => Right(doc)
        case Left(Seq(doc))  => Left(doc)
      }

  private def isVersionConflictException(
    bulkResponseItem: BulkResponseItem
  ): Boolean = {
    // This error is returned by Elasticsearch when we try to PUT a document
    // with a lower version than the existing version.
    val alreadyIndexedHasHigherVersion = bulkResponseItem.error
      .exists(
        bulkError =>
          bulkError.`type`.contains("version_conflict_engine_exception")
      )

    if (alreadyIndexedHasHigherVersion) {
      info(
        s"Skipping ${bulkResponseItem.id} because already indexed item has a higher version (${bulkResponseItem.error}"
      )
    }

    alreadyIndexedHasHigherVersion
  }
}
