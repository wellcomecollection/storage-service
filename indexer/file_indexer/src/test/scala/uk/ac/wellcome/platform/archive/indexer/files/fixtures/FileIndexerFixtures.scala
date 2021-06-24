package uk.ac.wellcome.platform.archive.indexer.files.fixtures

import java.time.Instant
import com.sksamuel.elastic4s.Index
import io.circe.Decoder
import org.scalatest.Suite
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.PrimaryS3StorageLocation
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  Indexer,
  IndexerWorker,
  StorageServiceIndexConfig
}
import uk.ac.wellcome.platform.archive.indexer.files.models.IndexedFile
import uk.ac.wellcome.platform.archive.indexer.files.{
  FileIndexer,
  FileIndexerWorker,
  FilesIndexConfig
}
import uk.ac.wellcome.platform.archive.indexer.fixtures.IndexerFixtures

import scala.concurrent.ExecutionContext.Implicits.global

trait FileIndexerFixtures
    extends IndexerFixtures[Seq[FileContext], FileContext, IndexedFile]
    with StorageManifestGenerators { this: Suite =>

  val indexConfig: StorageServiceIndexConfig = FilesIndexConfig

  def createContext: FileContext =
    FileContext(
      space = createStorageSpace,
      externalIdentifier = createExternalIdentifier,
      hashingAlgorithm = createChecksum.algorithm,
      bagLocation = PrimaryS3StorageLocation(
        createS3ObjectLocationPrefix
      ),
      file = createStorageManifestFile,
      createdDate = Instant.now
    )

  def createT: (Seq[FileContext], String) = {
    val context = createContext

    (Seq(context), context.location.toString())
  }

  def createIndexer(index: Index): Indexer[FileContext, IndexedFile] =
    new FileIndexer(client = elasticClient, index = index)

  override def withIndexerWorker[R](index: Index, queue: Queue)(
    testWith: TestWith[
      IndexerWorker[Seq[FileContext], FileContext, IndexedFile],
      R
    ]
  )(implicit decoder: Decoder[Seq[FileContext]]): R = {
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val worker = new FileIndexerWorker(
        config = createAlpakkaSQSWorkerConfig(queue),
        indexer = createIndexer(index),
        metricsNamespace = "indexer"
      )

      testWith(worker)
    }
  }

  def convertToIndexedT(contexts: Seq[FileContext]): IndexedFile =
    IndexedFile(contexts.head)
}
