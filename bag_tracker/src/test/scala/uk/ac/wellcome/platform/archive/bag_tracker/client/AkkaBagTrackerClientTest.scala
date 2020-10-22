package uk.ac.wellcome.platform.archive.bag_tracker.client

import akka.http.scaladsl.model.Uri
import org.scalatest.concurrent.IntegrationPatience
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.storage.StorageManifestDao
import uk.ac.wellcome.platform.archive.bag_tracker.storage.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

class AkkaBagTrackerClientTest
    extends BagTrackerClientTestCases
    with IntegrationPatience {
  override def withClient[R](
    trackerHost: String
  )(testWith: TestWith[BagTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      val client = new AkkaBagTrackerClient(trackerHost = Uri(trackerHost))

      testWith(client)
    }

  override def withStorageManifestDao[R](
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
}
