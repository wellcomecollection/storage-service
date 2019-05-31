package uk.ac.wellcome.platform.storage.bags.api.fixtures

import java.net.URL

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.platform.archive.common.fixtures.{
  HttpFixtures,
  RandomThings,
  StorageManifestVHSFixture
}
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.platform.storage.bags.api.BagsApi
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.memory.{
  MemoryConditionalUpdateDao,
  MemoryVersionedDao
}
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, Entry}

import scala.concurrent.ExecutionContext.Implicits.global

trait BagsApiFixture
    extends RandomThings
    with ScalaFutures
    with StorageManifestVHSFixture
    with HttpFixtures
    with MetricsSenderFixture
    with Akka {

  val metricsName = "BagsApiFixture"

  val contextURL = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json")

  private def withApp[R](
    metricsSender: MetricsSender,
    vhs: StorageManifestVersionedDao)(testWith: TestWith[BagsApi, R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit materializer =>
        val httpMetrics = new HttpMetrics(
          name = metricsName,
          metricsSender = metricsSender
        )

        val bagsApi = new BagsApi(
          vhs = vhs,
          httpMetrics = httpMetrics,
          httpServerConfig = httpServerConfig,
          contextURL = contextURL
        )

        bagsApi.run()

        testWith(bagsApi)
      }
    }

  def withConfiguredApp[R](
    testWith: TestWith[(StorageManifestVersionedDao, MetricsSender, String), R])
    : R = {
    val vhs = createStorageManifestDao()

    withMockMetricsSender { metricsSender =>
      withApp(metricsSender, vhs) { _ =>
        testWith((vhs, metricsSender, httpServerConfig.externalBaseURL))
      }
    }
  }

  def withBrokenApp[R](
    testWith: TestWith[(StorageManifestVersionedDao, MetricsSender, String), R])
    : R = {

    val brokenDao =
      new MemoryVersionedDao[String, Entry[String, EmptyMetadata]](
        underlying =
          MemoryConditionalUpdateDao[String, Entry[String, EmptyMetadata]]
      ) {
        override def get(
          id: String): scala.Either[ReadError, Entry[String, EmptyMetadata]] =
          Left(DaoReadError(new Throwable("BOOM!")))
      }

    val brokenVhs = createStorageManifestDao(dao = brokenDao)

    withMockMetricsSender { metricsSender =>
      withApp(metricsSender, brokenVhs) { _ =>
        testWith((brokenVhs, metricsSender, httpServerConfig.externalBaseURL))
      }
    }
  }
}
