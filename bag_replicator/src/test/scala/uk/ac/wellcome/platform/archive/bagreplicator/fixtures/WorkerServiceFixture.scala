package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.{NotificationStreamFixture, SNS}
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.services.{BagLocator, BagReplicator, BagReplicatorWorker}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.operation.OperationNotifier
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.storage.s3.S3PrefixCopier

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends NotificationStreamFixture
    with RandomThings
    with S3
    with SNS {
  def withWorkerService[R](queue: Queue = Queue(
                             "default_q",
                             "arn::default_q"
                           ),
                           ingestTopic: Topic,
                           outgoingTopic: Topic,
                           destination: ReplicatorDestinationConfig =
                             createReplicatorDestinationConfigWith(
                               Bucket(randomAlphanumeric())))(
    testWith: TestWith[BagReplicatorWorker, R]): R =
    withNotificationStream[BagRequest, R](queue) { notificationStream =>
      withSNSWriter(ingestTopic) { ingestSnsWriter =>
        withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
          val operationName = "replicating"

          val service = new BagReplicatorWorker(
            stream = notificationStream,
            notifier = new OperationNotifier(
              operationName,
              outgoingSnsWriter,
              ingestSnsWriter
            ),
            replicator = new BagReplicator(
              bagLocator = new BagLocator(s3Client),
              config = destination,
              s3PrefixCopier = S3PrefixCopier(s3Client)
            )
          )

          service.run()

          testWith(service)
        }
      }
    }

  def createReplicatorDestinationConfigWith(
    bucket: Bucket): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace = bucket.name,
      rootPath = Some(randomAlphanumeric())
    )
}
