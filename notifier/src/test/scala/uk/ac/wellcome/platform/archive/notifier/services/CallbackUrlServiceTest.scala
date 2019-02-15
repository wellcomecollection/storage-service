package uk.ac.wellcome.platform.archive.notifier.services

import java.net.{URI, URL}
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.ProgressGenerators
import uk.ac.wellcome.platform.archive.common.models.CallbackNotification
import uk.ac.wellcome.platform.archive.notifier.fixtures.{
  LocalWireMockFixture,
  NotifierFixture
}
import scala.concurrent.ExecutionContext.Implicits.global

class CallbackUrlServiceTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with Akka
    with LocalWireMockFixture
    with NotifierFixture
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

  private def withCallbackUrlService[R](
    testWith: TestWith[CallbackUrlService, R])(implicit actorSystem: ActorSystem): R = {
    val callbackUrlService = new CallbackUrlService(
      contextURL = new URL("http://localhost/context.json")
    )
    testWith(callbackUrlService)
  }
}
