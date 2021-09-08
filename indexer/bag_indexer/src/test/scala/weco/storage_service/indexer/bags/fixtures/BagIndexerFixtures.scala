package weco.storage_service.indexer.bag.fixtures

import com.sksamuel.elastic4s.Index
import io.circe.Decoder
import org.scalatest.Suite
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_tracker.fixtures.{
  BagTrackerFixtures,
  StorageManifestDaoFixture
}
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bagit.models.{BagVersion, ExternalIdentifier}
import weco.storage_service.generators.{
  IngestGenerators,
  PayloadGenerators,
  StorageManifestGenerators
}
import weco.storage_service.storage.models.{StorageManifest, StorageSpace}
import weco.storage_service.BagRegistrationNotification
import weco.storage_service.indexer.bags.models.IndexedStorageManifest
import weco.storage_service.indexer.bags.{
  BagIndexer,
  BagIndexerWorker,
  BagsIndexConfig
}
import weco.storage_service.indexer.{Indexer, IndexerWorker}
import weco.storage_service.indexer.fixtures.IndexerFixtures
import weco.storage_service.indexer.elasticsearch.StorageServiceIndexConfig

import scala.concurrent.ExecutionContext.Implicits.global

trait BagIndexerFixtures
    extends IndexerFixtures[
      BagRegistrationNotification,
      StorageManifest,
      IndexedStorageManifest
    ]
    with IngestGenerators
    with StorageManifestGenerators
    with StorageManifestDaoFixture
    with PayloadGenerators
    with BagTrackerFixtures { this: Suite =>

  def createIndexer(
    index: Index
  ): Indexer[StorageManifest, IndexedStorageManifest] =
    new BagIndexer(client = elasticClient, index = index)

  val indexConfig: StorageServiceIndexConfig = BagsIndexConfig

  val space: StorageSpace = createStorageSpace
  val externalIdentifier: ExternalIdentifier = createExternalIdentifier
  val version: BagVersion = BagVersion(1)

  val storageManifest: StorageManifest = createStorageManifestWith(
    space = space,
    externalIdentifier = externalIdentifier,
    version = version
  )
  val payload: BagRegistrationNotification = BagRegistrationNotification(
    storageManifest
  )

  def createT: (BagRegistrationNotification, String) =
    (payload, storageManifest.id.toString)

  def convertToIndexedT(
    notification: BagRegistrationNotification
  ): IndexedStorageManifest =
    IndexedStorageManifest(storageManifest)

  def withIndexerWorker[R](index: Index, queue: SQS.Queue)(
    testWith: TestWith[
      IndexerWorker[
        BagRegistrationNotification,
        StorageManifest,
        IndexedStorageManifest
      ],
      R
    ]
  )(implicit decoder: Decoder[BagRegistrationNotification]): R =
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val storageManifestDao: StorageManifestDao = createStorageManifestDao()

      val result = storageManifestDao.put(storageManifest)

      assert(result.isRight)

      withBagTrackerClient(storageManifestDao) { bagTrackerClient =>
        val worker = new BagIndexerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          indexer = createIndexer(index),
          metricsNamespace = "indexer",
          bagTrackerClient = bagTrackerClient
        )

        testWith(worker)
      }
    }
}
