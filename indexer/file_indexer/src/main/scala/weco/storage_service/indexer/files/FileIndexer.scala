package weco.storage_service.indexer.files

import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe.Encoder
import weco.storage_service.indexer.Indexer
import weco.storage_service.indexer.models.FileContext
import weco.storage_service.indexer.files.models.IndexedFile

import scala.concurrent.ExecutionContext

class FileIndexer(val client: ElasticClient, val index: Index)(
  implicit
  val ec: ExecutionContext,
  val encoder: Encoder[IndexedFile]
) extends Indexer[FileContext, IndexedFile] {
  override def id(file: FileContext): String = file.location.toString

  override protected def toDisplay(context: FileContext): IndexedFile =
    IndexedFile(context)

  override protected def version(context: FileContext): Long =
    context.createdDate.toEpochMilli
}
