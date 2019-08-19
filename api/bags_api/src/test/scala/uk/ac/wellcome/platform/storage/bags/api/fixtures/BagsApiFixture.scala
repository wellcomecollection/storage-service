package uk.ac.wellcome.platform.storage.bags.api.fixtures

import java.net.URL

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.fixtures.{HttpFixtures, StorageManifestVHSFixture, StorageRandomThings}
import uk.ac.wellcome.platform.archive.common.http.{HttpMetrics, WellcomeHttpApp}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.archive.common.storage.services.{EmptyMetadata, StorageManifestDao}
import uk.ac.wellcome.platform.storage.bags.api.BagsApi
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

trait BagsApiFixture
    extends StorageRandomThings
    with ScalaFutures
    with StorageManifestVHSFixture
    with HttpFixtures
    with MetricsSenderFixture {

  val metricsName = "BagsApiFixture"

  val contextURLTest = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json"
  )

  private def withApp[R](
    metrics: MemoryMetrics[Unit],
    storageManifestDaoTest: StorageManifestDao
  )(testWith: TestWith[WellcomeHttpApp, R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metrics = metrics
        )

        val router: BagsApi = new BagsApi {
          override implicit val ec: ExecutionContext = global
          override val contextURL: URL = contextURLTest
          override val storageManifestDao: StorageManifestDao =
            storageManifestDaoTest
        }

        val app = new WellcomeHttpApp(
          routes = router.bags,
          httpMetrics = httpMetrics,
          httpServerConfig = httpServerConfigTest,
          contextURL = contextURLTest
        )

        app.run()

        testWith(app)
      }
    }

  def withConfiguredApp[R](initialManifests: Seq[StorageManifest] = Seq.empty)(
    testWith: TestWith[(StorageManifestDao, MemoryMetrics[Unit], String), R]
  ): R = {
    val dao = createStorageManifestDao()

    initialManifests.foreach { manifest =>
      dao.put(manifest) shouldBe a[Right[_, _]]
    }

    val metrics = new MemoryMetrics[Unit]()

    withApp(metrics, dao) { _ =>
      testWith((dao, metrics, httpServerConfigTest.externalBaseURL))
    }
  }

  def withBrokenApp[R](
    testWith: TestWith[(MemoryMetrics[Unit], String), R]
  ): R = {
    val versionedStore = MemoryVersionedStore[BagId, HybridStoreEntry[
      StorageManifest,
      EmptyMetadata
    ]](
      initialEntries = Map.empty
    )

    val brokenDao = new MemoryStorageManifestDao(versionedStore) {
      override def getLatest(
        id: BagId
      ): scala.Either[ReadError, StorageManifest] =
        Left(StoreReadError(new Throwable("BOOM!")))

      override def get(
        id: BagId,
        version: BagVersion
      ): Either[ReadError, StorageManifest] =
        Left(StoreReadError(new Throwable("BOOM!")))

      override def listVersions(
        bagId: BagId
      ): Either[ReadError, Seq[StorageManifest]] =
        Left(StoreReadError(new Throwable("BOOM!")))
    }

    val metrics = new MemoryMetrics[Unit]()

    withApp(metrics, brokenDao) { _ =>
      testWith((metrics, httpServerConfigTest.externalBaseURL))
    }
  }
}
