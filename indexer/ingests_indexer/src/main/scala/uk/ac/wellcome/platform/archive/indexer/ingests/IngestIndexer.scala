package uk.ac.wellcome.platform.archive.indexer.ingests

import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer
import uk.ac.wellcome.platform.archive.indexer.ingests.models.IndexedIngest

import scala.concurrent.ExecutionContext

class IngestIndexer(val client: ElasticClient, val index: Index)(
  implicit
  val ec: ExecutionContext,
  val encoder: Encoder[IndexedIngest]
) extends Indexer[Ingest, IndexedIngest] {

  override def id(ingest: Ingest): String =
    ingest.id.underlying.toString

  override protected def toDisplay(ingest: Ingest): IndexedIngest =
    IndexedIngest(ingest)

  override protected def version(ingest: Ingest): Long =
    ingest.lastModifiedDate match {
      case Some(modifiedDate) => modifiedDate.toEpochMilli
      case None               => ingest.createdDate.toEpochMilli
    }
}
