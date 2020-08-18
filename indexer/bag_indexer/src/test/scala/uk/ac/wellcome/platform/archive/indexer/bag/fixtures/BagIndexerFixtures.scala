package uk.ac.wellcome.platform.archive.indexer.bag.fixtures

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import io.circe.Decoder
import org.scalatest.Suite
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.{
  BagTrackerFixtures,
  StorageManifestDaoFixture
}
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.generators.{
  IngestGenerators,
  PayloadGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
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
import uk.ac.wellcome.platform.archive.indexer.fixtures.IndexerFixtures

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

  val mapping: MappingDefinition = BagsIndexConfig.mapping

  val space: StorageSpace = createStorageSpace
  val externalIdentifier: ExternalIdentifier = createExternalIdentifier
  val version: BagVersion = BagVersion(1)

  val storageManifest: StorageManifest = createStorageManifestWith(
    space = space,
    version = version,
    bagInfo = createBagInfoWith(
      externalIdentifier = externalIdentifier
    )
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
      withFakeMonitoringClient() { implicit monitoringClient =>
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
}
