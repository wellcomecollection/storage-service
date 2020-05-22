package uk.ac.wellcome.platform.archive.bag_tracker.client

import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.BagTrackerApi
import uk.ac.wellcome.platform.archive.bag_tracker.models.{BagVersionEntry, BagVersionList}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{EmptyMetadata, StorageManifestDao}
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

trait BagTrackerClientTestCases extends AnyFunSpec with EitherValues with Matchers with ScalaFutures with Akka with StorageManifestGenerators {
  def withStorageManifestDao[R](initialManifests: Seq[StorageManifest])(testWith: TestWith[StorageManifestDao, R]): R = {
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

  def withApi[R](initialManifests: Seq[StorageManifest])(testWith: TestWith[BagTrackerApi, R]): R =
    withStorageManifestDao(initialManifests) { dao =>
      withApi(dao) { api =>
        testWith(api)
      }
    }

  val host: String = "localhost"
  val port: Int = 8080

  def withApi[R](dao: StorageManifestDao)(testWith: TestWith[BagTrackerApi, R]): R =
    withActorSystem { implicit actorSystem =>
      val api = new BagTrackerApi(dao)(host = host, port = port)

      api.run()

      testWith(api)
    }

  def withClient[R](testWith: TestWith[BagTrackerClient, R]): R

  describe("listVersionsOf") {
    it("finds a single version of a bag") {
      val manifest = createStorageManifest

      val expectedList = BagVersionList(
        id = manifest.id,
        versions = Seq(
          BagVersionEntry(version = manifest.version, createdDate = manifest.createdDate)
        )
      )

      withApi(initialManifests = Seq(manifest)) { _ =>
        withClient { client =>
          val future = client.listVersionsOf(manifest.id, before = None)

          whenReady(future) {
            _.right.value shouldBe expectedList
          }
        }
      }
    }

    it("finds multiple versions of a bag") {
      true shouldBe false
    }

    it("filters to versions before a given version") {
      true shouldBe false
    }

    it("returns Left[BagTrackerNotFoundListError] if there are no versions for this bag ID") {
      true shouldBe false
    }

    it("returns Left[BagTrackerNotFoundListError] if there are no versions before the given version") {
      true shouldBe false
    }

    it("returns Left[BagTrackerUnknownListError] if the API has an unexpected error") {
      true shouldBe false
    }
  }
}
