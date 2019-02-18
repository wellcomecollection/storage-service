package uk.ac.wellcome.platform.archive.progress_async.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.progress_async.services.CallbackNotificationService

import scala.concurrent.ExecutionContext.Implicits.global

trait CallbackNotificationServiceFixture extends SNS {
  def withCallbackNotificationService[R](topic: Topic)(testWith: TestWith[CallbackNotificationService, R]): R =
    withSNSWriter(topic) { snsWriter =>
      val service = new CallbackNotificationService(snsWriter)
      testWith(service)
    }
}
