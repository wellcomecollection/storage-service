package weco.storage_service.bags_api.fixtures

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import org.scalatest.concurrent.ScalaFutures
import weco.fixtures.TestWith
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_tracker.client.BagTrackerClient
import weco.storage_service.bag_tracker.fixtures.{
  BagTrackerFixtures,
  StorageManifestDaoFixture
}
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bag_tracker.storage.memory.MemoryStorageManifestDao
import weco.storage_service.bagit.models.{BagId, BagVersion}
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage_service.storage.models.StorageManifest
import weco.storage_service.bags_api.BagsApi
import weco.storage._
import weco.storage.fixtures.S3Fixtures
import weco.storage.s3.S3ObjectLocationPrefix
import weco.storage.services.s3.S3Uploader
import weco.storage.store.memory.MemoryVersionedStore
import weco.http.WellcomeHttpApp
import weco.http.fixtures.HttpFixtures
import weco.http.models.HTTPServerConfig
import weco.http.monitoring.HttpMetrics

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait BagsApiFixture
    extends StorageRandomGenerators
    with ScalaFutures
    with StorageManifestDaoFixture
    with S3Fixtures
    with HttpFixtures
    with BagTrackerFixtures {

  val metricsName = "BagsApiFixture"

  private def withBagsApi[R](
    metrics: MemoryMetrics,
    maxResponseByteLength: Long,
    locationPrefix: S3ObjectLocationPrefix,
    storageManifestDao: StorageManifestDao,
    uploader: S3Uploader
  )(testWith: TestWith[WellcomeHttpApp, R]): R = {

    withActorSystem { implicit actorSystem =>
      withMaterializer { implicit mat =>
        withBagTrackerClient(storageManifestDao) { trackerClient =>
          val httpMetrics = new HttpMetrics(
            name = metricsName,
            metrics = metrics
          )

          lazy val router: BagsApi = new BagsApi {
            override val httpServerConfig: HTTPServerConfig =
              httpServerConfigTest
            override implicit val ec: ExecutionContext = global

            override val bagTrackerClient: BagTrackerClient = trackerClient

            override val s3Uploader: S3Uploader = uploader
            override val s3Prefix: S3ObjectLocationPrefix = locationPrefix

            override val cacheDuration: Duration = 1 days
            override implicit val materializer: Materializer = mat
            override val maximumResponseByteLength: Long = maxResponseByteLength
          }

          withApp(router.bags, Some(httpMetrics), Some(actorSystem)) { app =>
            testWith(app)
          }
        }
      }
    }
  }

  def withConfiguredApp[R](
    initialManifests: Seq[StorageManifest] = Seq.empty,
    locationPrefix: S3ObjectLocationPrefix = createS3ObjectLocationPrefix,
    maxResponseByteLength: Long = 1048576
  )(
    testWith: TestWith[(StorageManifestDao, MemoryMetrics), R]
  )(implicit s3Client: AmazonS3): R = {
    val dao = createStorageManifestDao()
    val uploader = new S3Uploader()

    initialManifests.foreach { manifest =>
      dao.put(manifest) shouldBe a[Right[_, _]]
    }

    val metrics = new MemoryMetrics()

    withBagsApi(
      metrics = metrics,
      maxResponseByteLength = maxResponseByteLength,
      locationPrefix = locationPrefix,
      storageManifestDao = dao,
      uploader = uploader
    ) { _ =>
      testWith((dao, metrics))
    }
  }

  def withBrokenApp[R](
    testWith: TestWith[MemoryMetrics, R]
  ): R = {
    val versionedStore = MemoryVersionedStore[BagId, StorageManifest](
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
        bagId: BagId,
        before: Option[BagVersion]
      ): Either[ReadError, Seq[StorageManifest]] =
        Left(StoreReadError(new Throwable("BOOM!")))
    }

    val metrics = new MemoryMetrics()
    val maxResponseByteLength = 1048576
    val prefix = createS3ObjectLocationPrefix
    val uploader = new S3Uploader()

    withBagsApi(metrics, maxResponseByteLength, prefix, brokenDao, uploader) {
      _ =>
        testWith(metrics)
    }
  }

  private def whenRequestReady[R](
    r: HttpRequest
  )(testWith: TestWith[HttpResponse, R]): R =
    withActorSystem { implicit actorSystem =>
      val request = Http().singleRequest(r)
      whenReady(request) { response: HttpResponse =>
        testWith(response)
      }
    }

  def whenAbsoluteGetRequestReady[R](
    path: String
  )(testWith: TestWith[HttpResponse, R]): R = {
    val request = HttpRequest(method = GET, uri = path)

    whenRequestReady(request) { response =>
      testWith(response)
    }
  }
}
