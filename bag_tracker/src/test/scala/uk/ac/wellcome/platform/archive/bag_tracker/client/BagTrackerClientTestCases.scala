package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.BagTrackerApi
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

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
