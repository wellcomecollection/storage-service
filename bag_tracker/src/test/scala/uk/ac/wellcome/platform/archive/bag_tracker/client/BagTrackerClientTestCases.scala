package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.BagTrackerApi
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{
  EmptyMetadata,
  StorageManifestDao
}
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

trait BagTrackerClientTestBase extends Matchers with Akka {
  def withStorageManifestDao[R](
    initialManifests: Seq[StorageManifest]
  )(testWith: TestWith[StorageManifestDao, R]): R = {
    val versionedStore = MemoryVersionedStore[BagId, HybridStoreEntry[
      StorageManifest,
      EmptyMetadata
    ]](initialEntries = Map.empty)

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

  private val host: String = "localhost"
  private val port: Int = 8080

  val trackerHost: String = s"http://$host:$port"

  def withApi[R](
    dao: StorageManifestDao
  )(testWith: TestWith[BagTrackerApi, R]): R =
    withActorSystem { implicit actorSystem =>
      val api = new BagTrackerApi(dao)(host = host, port = port)

      api.run()

      testWith(api)
    }

  def withClient[R](trackerHost: String)(
    testWith: TestWith[BagTrackerClient, R]
  ): R
}

trait BagTrackerClientTestCases extends ListVersionsTestCases
