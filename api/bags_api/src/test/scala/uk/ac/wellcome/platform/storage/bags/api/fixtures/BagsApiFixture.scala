package uk.ac.wellcome.platform.storage.bags.api.fixtures

import java.net.URL

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.{
  HttpFixtures,
  StorageManifestVHSFixture,
  StorageRandomThings
}
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.storage.bags.api.BagsApi
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.HybridIndexedStoreEntry
import uk.ac.wellcome.storage.store.memory.MemoryStore

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

  private def withApp[R](metricsSender: MetricsSender, vhs: StorageManifestDao)(
    testWith: TestWith[BagsApi, R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metrics = metricsSender
        )

        val bagsApi = new BagsApi(
          register = vhs,
          httpMetrics = httpMetrics,
          httpServerConfig = httpServerConfig,
          contextURL = contextURL
        )

        bagsApi.run()

        testWith(bagsApi)
      }
    }

  def withConfiguredApp[R](initialManifests: Seq[StorageManifest] = Seq.empty)(
    testWith: TestWith[(StorageManifestDao, MetricsSender, String), R]): R = {
    val dao = createStorageManifestDao()

    initialManifests.foreach { manifest =>
      dao.put(manifest) shouldBe a[Right[_, _]]
    }

    withMockMetricsSender { metricsSender =>
      withApp(metricsSender, dao) { _ =>
        testWith((dao, metricsSender, httpServerConfig.externalBaseURL))
      }
    }
  }

  def withBrokenApp[R](
    testWith: TestWith[(StorageManifestDao, MetricsSender, String), R]): R = {

    // TODO: This should be a MaximaReadError really.
    val brokenIndex =
      new MemoryStore[
        Version[BagId, Int],
        HybridIndexedStoreEntry[Version[BagId, Int],
                                String,
                                Map[String, String]]](
        initialEntries = Map.empty) with MemoryMaxima[
        BagId,
        HybridIndexedStoreEntry[Version[BagId, Int],
                                String,
                                Map[String, String]]] {
        override def max(id: BagId) =
          Left(NoMaximaValueError(new Throwable("BOOM!")))
      }

    val brokenVhs = createStorageManifestDao(indexStore = brokenIndex)

    withMockMetricsSender { metricsSender =>
      withApp(metricsSender, brokenVhs) { _ =>
        testWith((brokenVhs, metricsSender, httpServerConfig.externalBaseURL))
      }
    }
  }
}
