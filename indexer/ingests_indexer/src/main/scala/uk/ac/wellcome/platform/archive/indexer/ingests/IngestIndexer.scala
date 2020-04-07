package uk.ac.wellcome.platform.archive.indexer.ingests

import java.net.URL

import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.display.ResponseDisplayIngest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer

import scala.concurrent.ExecutionContext

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
