package uk.ac.wellcome.platform.archive.notifier.services

import java.net.URI
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.platform.archive.common.generators.ProgressGenerators
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.notifier.fixtures.{CallbackUrlServiceFixture, LocalWireMockFixture, WorkerServiceFixture}

class CallbackUrlServiceTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with Akka
    with CallbackUrlServiceFixture
    with LocalWireMockFixture
    with ProgressGenerators {

  it("returns a Success if the request succeeds") {
    withActorSystem { implicit actorSystem =>
      withCallbackUrlService { service =>
        val requestId = UUID.randomUUID()
        val future = service.getHttpResponse(
          callbackNotification = CallbackNotification(
            id = requestId,
            callbackUri = new URI(s"http://$callbackHost:$callbackPort/callback/$requestId"),
            payload = createProgress
          )
        )

        whenReady(future) { result =>
          result.isSuccess shouldBe true
          result.get.status shouldBe StatusCodes.NotFound
        }
      }
    }
  }

  it("returns a failed future if the HTTP request fails") {
    withActorSystem { implicit actorSystem =>
      withCallbackUrlService { service =>
        val requestId = UUID.randomUUID()
        val future = service.getHttpResponse(
          callbackNotification = CallbackNotification(
            id = requestId,
            callbackUri = new URI(s"http://nope.nope/callback/$requestId"),
            payload = createProgress
          )
        )

        whenReady(future) { result =>
          result.isFailure shouldBe true
        }
      }
    }
  }

  // TODO: Add a test that it sends the correct POST payload.
}
