package uk.ac.wellcome.platform.archive.bag_tracker.fixtures

import akka.http.scaladsl.model.Uri
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bag_tracker.BagTrackerApi
import uk.ac.wellcome.platform.archive.bag_tracker.client.{
  AkkaBagTrackerClient,
  BagTrackerClient
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao

trait BagTrackerFixtures extends Akka {
  private val host: String = "localhost"
  private val port: Int = 8080

  val trackerHost: String = s"http://$host:$port"

  def withApi[R](dao: StorageManifestDao)(testWith: TestWith[BagTrackerApi, R]): R =
    withActorSystem { implicit actorSystem =>
      val api = new BagTrackerApi(dao)(host = host, port = port)

      api.run()

      testWith(api)
    }

  def withBagTrackerClient[R](storageManifestDao: StorageManifestDao)(testWith: TestWith[BagTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      withApi(storageManifestDao) { _ =>
        val client = new AkkaBagTrackerClient(trackerHost = Uri(trackerHost))

        testWith(client)
      }
    }
}
