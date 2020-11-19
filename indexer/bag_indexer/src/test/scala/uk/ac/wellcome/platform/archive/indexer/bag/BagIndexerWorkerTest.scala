package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.Index
import io.circe.Decoder
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.worker.models.NonDeterministicFailure
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.bag_tracker.storage.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.IndexerWorkerTestCases
import uk.ac.wellcome.platform.archive.indexer.bag.fixtures.BagIndexerFixtures
import uk.ac.wellcome.platform.archive.indexer.bags.BagIndexerWorker
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.IndexerWorker
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore
import uk.ac.wellcome.storage.{DoesNotExistError, ReadError, StoreReadError}

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
          metricsNamespace = "indexer",
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
          metricsNamespace = "indexer",
          bagTrackerClient = trackerClient
        )

        testWith(worker)
      }
    }
  }

  it(
    "fails with a NonDeterministicFailure when a StoreReadError is encountered"
  ) {
    val (t, _) = createT
    withLocalElasticsearchIndex(mapping) { index =>
      withLocalSqsQueue() { queue =>
        withStoreReadErrorIndexerWorker(index, queue) { worker =>
          whenReady(worker.process(t)) {
            _ shouldBe a[NonDeterministicFailure[_]]
          }
        }
      }
    }
  }

  it("fails with a NonDeterministicFailure if a bag doesn't exist") {
    val (t, _) = createT
    withLocalElasticsearchIndex(mapping) { index =>
      withLocalSqsQueue() { queue =>
        withDoesNotExistErrorIndexerWorker(index, queue) { worker =>
          whenReady(worker.process(t)) {
            _ shouldBe a[NonDeterministicFailure[_]]
          }
        }
      }
    }
  }
}
