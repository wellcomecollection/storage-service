package weco.storage_service.bag_tracker.fixtures

import org.apache.pekko.http.scaladsl.model.Uri
import weco.pekko.fixtures.Pekko
import weco.fixtures.TestWith
import weco.storage_service.bag_tracker.BagTrackerApi
import weco.storage_service.bag_tracker.client.{
  PekkoBagTrackerClient,
  BagTrackerClient
}
import weco.storage_service.bag_tracker.storage.StorageManifestDao

import scala.concurrent.ExecutionContext.Implicits.global

trait BagTrackerFixtures extends Pekko {
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

  def withBagTrackerClient[R](
    storageManifestDao: StorageManifestDao
  )(testWith: TestWith[BagTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      withApi(storageManifestDao) { _ =>
        val client = new PekkoBagTrackerClient(trackerHost = Uri(trackerHost))

        testWith(client)
      }
    }
}
