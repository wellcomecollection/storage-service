package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.bagreplicator.config.{BagReplicatorConfig, ReplicatorDestinationConfig}
import uk.ac.wellcome.platform.archive.bagreplicator.services.{BagReplicatorWorkerService, BagStorageService}
import uk.ac.wellcome.platform.archive.bagreplicator.storage.{S3Copier, S3PrefixCopier}
import uk.ac.wellcome.platform.archive.common.fixtures.NotificationStreamFixture
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture extends NotificationStreamFixture with S3 with SNS {
  def withWorkerService[R](queue: Queue = Queue("default_q", "arn::default_q"),
                           progressTopic: Topic,
                           outgoingTopic: Topic)(
    testWith: TestWith[BagReplicatorWorkerService, R]): R = {
    val s3PrefixCopier = new S3PrefixCopier(
      s3Client = s3Client,
      copier = new S3Copier(s3Client = s3Client)
    )

    val bagStorageService = new BagStorageService(
      s3PrefixCopier = s3PrefixCopier
    )

    withNotificationStream[BagRequest, R](queue) { notificationStream =>
      withSNSWriter(progressTopic) { progressSnsWriter =>
        withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
          withLocalS3Bucket { dstBucket =>
            val service = new BagReplicatorWorkerService(
              notificationStream = notificationStream,
              bagStorageService = bagStorageService,
              bagReplicatorConfig = BagReplicatorConfig(
                parallelism = 1,
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
