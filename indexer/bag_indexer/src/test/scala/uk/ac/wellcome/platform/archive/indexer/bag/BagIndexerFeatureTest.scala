package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import io.circe.Decoder
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.{
  KnownReplicasPayload,
  PipelineContext
}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.{
  IngestGenerators,
  PayloadGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.archive.indexer.IndexerFeatureTestCases
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.bags.{
  BagIndexer,
  BagIndexerWorker,
  BagsIndexConfig
}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.{
  Indexer,
  IndexerWorker
}

import scala.concurrent.ExecutionContext.Implicits.global

class BagIndexerFeatureTest
    extends IndexerFeatureTestCases[
      KnownReplicasPayload,
      StorageManifest,
      IndexedStorageManifest
    ]
    with StorageManifestGenerators
    with PayloadGenerators
    with IngestGenerators
    with StorageManifestVHSFixture {

  def createIndexer(
    index: Index
  ): Indexer[StorageManifest, IndexedStorageManifest] =
    new BagIndexer(
      client = elasticClient,
      index = index
    )

  override val mapping: MappingDefinition = BagsIndexConfig.mapping

  val version: BagVersion = BagVersion(1)
  val ingest: Ingest = createIngestWith(version = Some(version))
  val pipelineContext: PipelineContext = PipelineContext(ingest)

  val bagInfo = createBagInfoWith(
    externalIdentifier = ingest.externalIdentifier
  )

  val storageManifest: StorageManifest = createStorageManifestWith(
    ingestId = ingest.id,
    space = ingest.space,
    version = version,
    bagInfo = bagInfo
  )
  val payload: KnownReplicasPayload = createKnownReplicasPayloadWith(
    context = pipelineContext,
    version = version
  )

  override def createT: (KnownReplicasPayload, String) = {
    val bagId = BagId(
      space = payload.storageSpace,
      externalIdentifier = payload.externalIdentifier
    )

    (payload, bagId.toString)
  }

  override def convertIndexedT(
    payload: KnownReplicasPayload
  ): IndexedStorageManifest = {
    IndexedStorageManifest(storageManifest)
  }

  override def withIndexerWorker[R](index: Index, queue: SQS.Queue)(
    testWith: TestWith[
      IndexerWorker[
        KnownReplicasPayload,
        StorageManifest,
        IndexedStorageManifest
      ],
      R
    ]
  )(implicit decoder: Decoder[KnownReplicasPayload]): R = {
    withActorSystem { implicit actorSystem =>
      withFakeMonitoringClient() { implicit monitoringClient =>
        val storageManifestDao: StorageManifestDao = createStorageManifestDao()

        val result = storageManifestDao.put(storageManifest)

        assert(result.isRight)

        val worker = new BagIndexerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          indexer = createIndexer(index),
          metricsNamespace = "indexer",
          storageManifestDao = storageManifestDao
        )

        testWith(worker)
      }
    }
  }
}
