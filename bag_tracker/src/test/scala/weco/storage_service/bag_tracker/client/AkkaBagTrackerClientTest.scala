package weco.storage_service.bag_tracker.client

import org.apache.pekko.http.scaladsl.model.Uri
import org.scalatest.concurrent.IntegrationPatience
import weco.fixtures.TestWith

import scala.concurrent.ExecutionContext.Implicits.global

class PekkoBagTrackerClientTest
    extends BagTrackerClientTestCases
    with IntegrationPatience {
  override def withClient[R](
    trackerHost: String
  )(testWith: TestWith[BagTrackerClient, R]): R =
    withActorSystem { implicit actorSystem =>
      val client = new PekkoBagTrackerClient(trackerHost = Uri(trackerHost))

      testWith(client)
    }
}
