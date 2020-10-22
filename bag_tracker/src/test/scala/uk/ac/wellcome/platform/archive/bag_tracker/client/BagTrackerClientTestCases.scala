package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.BagTrackerApi
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

trait BagTrackerClientTestBase extends Matchers with BagTrackerFixtures {
  def withStorageManifestDao[R](
    initialManifests: Seq[StorageManifest]
  )(testWith: TestWith[StorageManifestDao, R]): R

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
