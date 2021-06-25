package weco.storage_service.bag_tracker.client

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import weco.fixtures.TestWith
import weco.storage_service.bag_tracker.BagTrackerApi
import weco.storage_service.bag_tracker.fixtures.BagTrackerFixtures
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.bagit.models.BagId
import weco.storage_service.storage.models.StorageManifest
import weco.storage.store.memory.MemoryVersionedStore

trait BagTrackerClientTestBase extends Matchers with BagTrackerFixtures {
  def withStorageManifestDao[R](
    initialManifests: Seq[StorageManifest]
  )(testWith: TestWith[StorageManifestDao, R]): R = {
    val versionedStore =
      MemoryVersionedStore[BagId, StorageManifest](initialEntries = Map.empty)

    val dao = new MemoryStorageManifestDao(versionedStore)

    initialManifests.foreach {
      dao.put(_) shouldBe a[Right[_, _]]
    }

    testWith(dao)
  }

  def withApi[R](
    initialManifests: Seq[StorageManifest] = Seq.empty
  )(testWith: TestWith[BagTrackerApi, R]): R =
    withStorageManifestDao(initialManifests) { dao =>
      withApi(dao) { api =>
        testWith(api)
      }
    }

  def withClient[R](trackerHost: String)(
    testWith: TestWith[BagTrackerClient, R]
  ): R
}

trait BagTrackerClientTestCases
    extends CreateBagTestCases
    with GetBagTestCases
    with GetLatestBagTestCases
    with ListVersionsTestCases
    with TableDrivenPropertyChecks {

  val unusualIdentifiers: TableFor1[String] = Table(
    "externalIdentifier",
    "alfa/bravo",
    "miro images",
    "miro/A images"
  )
}
