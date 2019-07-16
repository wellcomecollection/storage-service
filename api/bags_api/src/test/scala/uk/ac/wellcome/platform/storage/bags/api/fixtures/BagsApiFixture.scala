package uk.ac.wellcome.platform.storage.bags.api.fixtures

import java.net.URL

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.{
  HttpFixtures,
  StorageManifestVHSFixture,
  StorageRandomThings
}
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.{
  EmptyMetadata,
  StorageManifestDao
}
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.storage.bags.api.BagsApi
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

import scala.concurrent.ExecutionContext.Implicits.global

trait BagsApiFixture
    extends StorageRandomThings
    with ScalaFutures
    with StorageManifestVHSFixture
    with HttpFixtures
    with MetricsSenderFixture {

  val metricsName = "BagsApiFixture"

  val contextURL = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json")

  private def withApp[R](
    metrics: MemoryMetrics[Unit],
    storageManifestDao: StorageManifestDao)(testWith: TestWith[BagsApi, R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metrics = metrics
        )

        val bagsApi = new BagsApi(
          storageManifestDao = storageManifestDao,
          httpMetrics = httpMetrics,
          httpServerConfig = httpServerConfig,
          contextURL = contextURL
        )

        bagsApi.run()

        testWith(bagsApi)
      }
    }

  def withConfiguredApp[R](initialManifests: Seq[StorageManifest] = Seq.empty)(
    testWith: TestWith[(StorageManifestDao, MemoryMetrics[Unit], String), R])
    : R = {
    val dao = createStorageManifestDao()

    initialManifests.foreach { manifest =>
      dao.put(manifest) shouldBe a[Right[_, _]]
    }

    val metrics = new MemoryMetrics[Unit]()

    withApp(metrics, dao) { _ =>
      testWith((dao, metrics, httpServerConfig.externalBaseURL))
    }
  }

  def withBrokenApp[R](
    testWith: TestWith[(StorageManifestDao, MemoryMetrics[Unit], String), R])
    : R = {

    // TODO: This should be a MaximaReadError really.
    val brokenIndex =
      new MemoryStore[
        Version[BagId, Int],
        HybridStoreEntry[StorageManifest, EmptyMetadata]](
        initialEntries = Map.empty) with MemoryMaxima[
        BagId,
        HybridStoreEntry[StorageManifest, EmptyMetadata]] {
        override def max(id: BagId) =
          Left(NoMaximaValueError(new Throwable("BOOM!")))
      }

    val brokenVhs = new MemoryStorageManifestDao(
      new MemoryVersionedStore[
        BagId,
        HybridStoreEntry[StorageManifest, EmptyMetadata]](
        brokenIndex
      )
    )

    val metrics = new MemoryMetrics[Unit]()

    withApp(metrics, brokenVhs) { _ =>
      testWith((brokenVhs, metrics, httpServerConfig.externalBaseURL))
    }
  }
}
