package uk.ac.wellcome.platform.archive.archivist.fixtures

import akka.actor.ActorSystem
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.archivist.messaging.MessageStream

trait ArchiveMessaging extends SQS {
  def withArchiveMessageStream[I, O, R](queue: Queue)(
    testWith: TestWith[MessageStream[I, O], R])(
    implicit actorSystem: ActorSystem): R =
    withMockMetricSender { mockMetricsSender =>
      val messageStream = new MessageStream[I, O](
        sqsClient = asyncSqsClient,
        sqsConfig = createSQSConfigWith(queue),
        metricsSender = mockMetricsSender
      )

      testWith(messageStream)
    }
}
