package uk.ac.wellcome.platform.archive.indexer.ingests

import java.net.URL

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import com.sksamuel.elastic4s.{ElasticClient, Index, Response}
import grizzled.slf4j.Logging
import io.circe._
import io.circe.parser._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.display.ResponseDisplayIngest

import scala.concurrent.{ExecutionContext, Future}

trait Indexer[Document, DisplayDocument] extends Logging {
  val client: ElasticClient
  val index: Index

  implicit val ec: ExecutionContext
  implicit val encoder: Encoder[DisplayDocument]

  protected def id(doc: Document): String
  protected def toDisplay(doc: Document): DisplayDocument

  final def index(documents: Seq[Document]): Future[Either[Seq[Document], Seq[Document]]] = {
    debug(s"Indexing documents: ${documents.map { id }.mkString(", ")}")

    val inserts = documents.map { doc =>
      indexInto(index)
        .id { id(doc) }
        .doc { asJson(toDisplay(doc)) }
    }

    client
      .execute { bulk(inserts) }
      .map { response: Response[BulkResponse] =>
        if (response.isError || response.result.errors) {
          error(s"Error from Elasticsearch: $response")
          Left(documents)
        } else {
          val failedIds = response.result
            .failures
            .map { _.id }
            .toSet

          if (failedIds.isEmpty) {
            Right(documents)
          } else {
            val failedDocuments = documents.filter { doc => failedIds.contains(id(doc)) }

            Right(failedDocuments)
          }
        }
      }
  }

  private def asJson(displayDocument: DisplayDocument): String = {
    val jsonString = toJson(displayDocument).get

    removeContextUrl(jsonString)
  }

  // Our Display models include a context URL that we don't need to index, so
  // take that out before actually indexing the document.
  //
  // If we can't find a context URL, just return the original string.
  private def removeContextUrl(jsonString: String): String = {
    parse(jsonString) match {
      case Right(value) => value.asObject match {
        case Some(jsonObject: JsonObject) =>
          Json
            .fromJsonObject(jsonObject.remove("@context"))
            .noSpaces

        case None => jsonString
      }

      case Left(_) => jsonString
    }
  }
}


class IngestIndexer(
  val client: ElasticClient,
  val index: Index)(
  implicit
  val ec: ExecutionContext,
  val encoder: Encoder[ResponseDisplayIngest]
) extends Indexer[Ingest, ResponseDisplayIngest] {

  override protected def id(ingest: Ingest): String = ingest.id.underlying.toString

  override protected def toDisplay(ingest: Ingest): ResponseDisplayIngest =
    ResponseDisplayIngest(
      ingest = ingest,
      contextUrl = new URL("http://localhost:9200")
    )
}
