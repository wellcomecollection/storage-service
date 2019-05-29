package uk.ac.wellcome.platform.storage.bags.api.fixtures

import java.net.URL

import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.platform.archive.common.fixtures.{HttpFixtures, RandomThings, StorageManifestVHSFixture}
import uk.ac.wellcome.platform.archive.common.http.HttpMetrics
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestVHS
import uk.ac.wellcome.platform.storage.bags.api.BagsApi
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.memory.MemoryObjectStore
import uk.ac.wellcome.storage.streaming.CodecInstances._

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

  private def withApp[R](metricsSender: MetricsSender, vhs: StorageManifestVHS)(
    testWith: TestWith[BagsApi, R]): R =
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
    testWith: TestWith[(StorageManifestVHS, MetricsSender, String), R]): R = {
    val vhs = createStorageManifestVHS()

    withMockMetricsSender { metricsSender =>
      withApp(metricsSender, vhs) { _ =>
        testWith((vhs, metricsSender, httpServerConfig.externalBaseURL))
      }
    }
  }

  def withBrokenApp[R](
    testWith: TestWith[(StorageManifestVHS, MetricsSender, String), R]): R = {

    val brokenStore = new MemoryObjectStore[StorageManifest]() {
      override def put(namespace: String)(
        input: StorageManifest,
        keyPrefix: KeyPrefix,
        keySuffix: KeySuffix,
        userMetadata: Map[String, String]
      ): Either[WriteError, ObjectLocation] =
        Left(BackendWriteError(new Throwable("BOOM!")))
    }

    val brokenVhs = createStorageManifestVHS(store = brokenStore)

    withMockMetricsSender { metricsSender =>
      withApp(metricsSender, brokenVhs) { _ =>
        testWith((brokenVhs, metricsSender, httpServerConfig.externalBaseURL))
      }
    }
  }
}
