package uk.ac.wellcome.platform.storage.bags.api.fixtures

import java.net.URL

import akka.stream.Materializer
import com.amazonaws.services.s3.AmazonS3
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.bag_tracker.client.BagTrackerClient
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.platform.archive.common.fixtures.{
  HttpFixtures,
  StorageManifestDaoFixture,
  StorageRandomThings
}
import uk.ac.wellcome.platform.archive.common.http.{
  HttpMetrics,
  WellcomeHttpApp
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemoryStorageManifestDao
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3Uploader
import uk.ac.wellcome.platform.archive.common.storage.services.{
  EmptyMetadata,
  StorageManifestDao
}
import uk.ac.wellcome.platform.storage.bags.api.BagsApi
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait BagsApiFixture
    extends StorageRandomThings
    with ScalaFutures
    with StorageManifestDaoFixture
    with NewS3Fixtures
    with HttpFixtures
    with BagTrackerFixtures {

  override val metricsName = "BagsApiFixture"

  override val contextURLTest = new URL(
    "http://api.wellcomecollection.org/storage/v1/context.json"
  )

  private def withApp[R](
    metrics: MemoryMetrics[Unit],
    maxResponseByteLength: Long,
    locationPrefix: S3ObjectLocationPrefix,
    storageManifestDao: StorageManifestDao,
    uploader: S3Uploader
  )(testWith: TestWith[WellcomeHttpApp, R]): R =
    withActorSystem { implicit actorSystem =>
      withMaterializer(actorSystem) { implicit mat =>
        withBagTrackerClient(storageManifestDao) { trackerClient =>
          val httpMetrics = new HttpMetrics(
            name = metricsName,
            metrics = metrics
          )

          lazy val router: BagsApi = new BagsApi {
            override val httpServerConfig: HTTPServerConfig =
              httpServerConfigTest
            override implicit val ec: ExecutionContext = global
            override val contextURL: URL = contextURLTest

            override val bagTrackerClient: BagTrackerClient = trackerClient

            override val s3Uploader: S3Uploader = uploader
            override val s3Prefix: S3ObjectLocationPrefix = locationPrefix

            override val cacheDuration: Duration = 1 days
            override implicit val materializer: Materializer = mat
            override val maximumResponseByteLength: Long = maxResponseByteLength
          }

          val app = new WellcomeHttpApp(
            routes = router.bags,
            httpMetrics = httpMetrics,
            httpServerConfig = httpServerConfigTest,
            contextURL = contextURLTest,
            appName = metricsName
          )

          app.run()

          testWith(app)
        }
      }
    }

  def withConfiguredApp[R](
    initialManifests: Seq[StorageManifest] = Seq.empty,
    locationPrefix: S3ObjectLocationPrefix = createS3ObjectLocationPrefix,
    maxResponseByteLength: Long = 1048576
  )(
    testWith: TestWith[(StorageManifestDao, MemoryMetrics[Unit], String), R]
  )(implicit s3Client: AmazonS3): R = {
    val dao = createStorageManifestDao()
    val uploader = new S3Uploader()

    initialManifests.foreach { manifest =>
      dao.put(manifest) shouldBe a[Right[_, _]]
    }

    val metrics = new MemoryMetrics[Unit]()

    withApp(
      metrics = metrics,
      maxResponseByteLength = maxResponseByteLength,
      locationPrefix = locationPrefix,
      storageManifestDao = dao,
      uploader = uploader
    ) { _ =>
      testWith((dao, metrics, httpServerConfigTest.externalBaseURL))
    }
  }

  def withBrokenApp[R](
    testWith: TestWith[(MemoryMetrics[Unit], String), R]
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

    val metrics = new MemoryMetrics[Unit]()
    val maxResponseByteLength = 1048576
    val prefix = createS3ObjectLocationPrefix
    val uploader = new S3Uploader()

    withApp(metrics, maxResponseByteLength, prefix, brokenDao, uploader) { _ =>
      testWith((metrics, httpServerConfigTest.externalBaseURL))
    }
  }
}
