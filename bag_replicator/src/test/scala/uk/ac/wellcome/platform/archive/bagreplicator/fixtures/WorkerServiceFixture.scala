package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.{NotificationStreamFixture, SNS}
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.bagreplicator.config.{
  BagReplicatorConfig,
  ReplicatorDestinationConfig
}
import uk.ac.wellcome.platform.archive.bagreplicator.services.{
  BagReplicatorWorkerService,
  BagStorageService
}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.s3.S3PrefixCopier

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture extends NotificationStreamFixture with S3 with SNS {
  def withWorkerService[R](queue: Queue = Queue("default_q", "arn::default_q"),
                           progressTopic: Topic,
                           outgoingTopic: Topic)(
    testWith: TestWith[BagReplicatorWorkerService, R]): R = {
    val bagStorageService = new BagStorageService(
      s3PrefixCopier = S3PrefixCopier(s3Client)
    )

    withNotificationStream[BagRequest, R](queue) { notificationStream =>
      withSNSWriter(progressTopic) { progressSnsWriter =>
        withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
          withLocalS3Bucket { dstBucket =>
            val service = new BagReplicatorWorkerService(
              notificationStream = notificationStream,
              bagStorageService = bagStorageService,
              bagReplicatorConfig = BagReplicatorConfig(
                destination = ReplicatorDestinationConfig(
                  namespace = dstBucket.name,
                  rootPath = Some("destinations/")
                )
              ),
              progressSnsWriter = progressSnsWriter,
              outgoingSnsWriter = outgoingSnsWriter
            )

            service.run()

            testWith(service)
          }
        }
      }
    }
  }
}
