package uk.ac.wellcome.platform.archive.notifier.fixtures

import java.net.URL

import akka.actor.ActorSystem
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.notifier.services.CallbackUrlService

import scala.concurrent.ExecutionContext.Implicits.global

trait CallbackUrlServiceFixture {
  def withCallbackUrlService[R](
    testWith: TestWith[CallbackUrlService, R])(
    implicit actorSystem: ActorSystem): R = {
    val callbackUrlService = new CallbackUrlService(
      contextURL = new URL("http://localhost/context.json")
    )
    testWith(callbackUrlService)
  }
}
