package uk.ac.wellcome.platform.archive.ingests.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.platform.archive.ingests.services.CallbackNotificationService

trait CallbackNotificationServiceFixture extends SNS {
  def withCallbackNotificationService[R](messageSender: MessageSender[String])(
    testWith: TestWith[CallbackNotificationService[String], R]): R = {
    val service = new CallbackNotificationService(messageSender)
    testWith(service)
  }
}
