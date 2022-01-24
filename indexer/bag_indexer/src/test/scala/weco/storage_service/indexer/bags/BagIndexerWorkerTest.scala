package weco.storage_service.indexer.bags

import com.sksamuel.elastic4s.Index
import io.circe.Decoder
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS
import weco.messaging.worker.models.RetryableFailure
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.BagRegistrationNotification
import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage_service.storage.models.StorageManifest
import weco.storage_service.indexer.IndexerWorkerTestCases
import weco.storage_service.indexer.bag.fixtures.BagIndexerFixtures
import weco.storage_service.indexer.bags.models.IndexedStorageManifest
import weco.storage_service.indexer.IndexerWorker
import weco.storage.store.memory.MemoryVersionedStore
import weco.storage.{DoesNotExistError, ReadError, StoreReadError}

class BagIndexerWorkerTest
    extends IndexerWorkerTestCases[
      BagRegistrationNotification,
      StorageManifest,
      IndexedStorageManifest
    ]
    with BagIndexerFixtures {

  def withStoreReadErrorIndexerWorker[R](index: Index, queue: SQS.Queue)(
    testWith: TestWith[
      IndexerWorker[
        BagRegistrationNotification,
        StorageManifest,
        IndexedStorageManifest
      ],
      R
    ]
  )(implicit decoder: Decoder[BagRegistrationNotification]): R = {
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val storageManifestDao: StorageManifestDao =
        new MemoryStorageManifestDao(
          MemoryVersionedStore[BagId, StorageManifest](
            initialEntries = Map.empty
          )
        ) {
          override def get(
            id: BagId,
            version: BagVersion
          ): Either[ReadError, StorageManifest] = {
            Left(StoreReadError(new Exception("BOOM!")))
          }
        }
      withBagTrackerClient(storageManifestDao) { trackerClient =>
        val worker = new BagIndexerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          indexer = createIndexer(index),
          bagTrackerClient = trackerClient
        )

        testWith(worker)
      }
    }
  }

  def withDoesNotExistErrorIndexerWorker[R](index: Index, queue: SQS.Queue)(
    testWith: TestWith[
      IndexerWorker[
        BagRegistrationNotification,
        StorageManifest,
        IndexedStorageManifest
      ],
      R
    ]
  )(implicit decoder: Decoder[BagRegistrationNotification]): R = {
    withActorSystem { implicit actorSystem =>
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val storageManifestDao: StorageManifestDao =
        new MemoryStorageManifestDao(
          MemoryVersionedStore[BagId, StorageManifest](
            initialEntries = Map.empty
          )
        ) {
          override def get(
            id: BagId,
            version: BagVersion
          ): Either[ReadError, StorageManifest] = {
            Left(DoesNotExistError(new Exception("BOOM!")))
          }
        }
      withBagTrackerClient(storageManifestDao) { trackerClient =>
        val worker = new BagIndexerWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          indexer = createIndexer(index),
          bagTrackerClient = trackerClient
        )

        testWith(worker)
      }
    }
  }

  it("fails with a retryable failure when it can't read from the store"
  ) {
    val (t, _) = createT
    withLocalElasticsearchIndex(indexConfig) { index =>
      withLocalSqsQueue() { queue =>
        withStoreReadErrorIndexerWorker(index, queue) { worker =>
          whenReady(worker.process(t)) {
            _ shouldBe a[RetryableFailure[_]]
          }
        }
      }
    }
  }

  it("fails with a retryable failure if a bag doesn't exist") {
    val (t, _) = createT
    withLocalElasticsearchIndex(indexConfig) { index =>
      withLocalSqsQueue() { queue =>
        withDoesNotExistErrorIndexerWorker(index, queue) { worker =>
          whenReady(worker.process(t)) {
            _ shouldBe a[RetryableFailure[_]]
          }
        }
      }
    }
  }
}
