package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.{SNS, SQS}
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.bagreplicator.config.{
  BagReplicatorConfig,
  ReplicatorDestinationConfig
}
import uk.ac.wellcome.platform.archive.bagreplicator.services.{
  BagReplicatorWorkerService,
  BagStorageService
}
import uk.ac.wellcome.platform.archive.bagreplicator.storage.{
  S3Copier,
  S3PrefixCopier
}
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture extends Akka with S3 with SNS with SQS {
  def withWorkerService[R](queue: Queue,
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

    withActorSystem { implicit actorSystem =>
      withSQSStream[NotificationMessage, R](queue) { sqsStream =>
        withSNSWriter(progressTopic) { progressSnsWriter =>
          withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
            withLocalS3Bucket { dstBucket =>
              val service = new BagReplicatorWorkerService(
                sqsStream = sqsStream,
                bagStorageService = bagStorageService,
                bagReplicatorConfig = BagReplicatorConfig(
                  parallelism = 1,
                  destination = ReplicatorDestinationConfig(
                    namespace = dstBucket.name,
                    rootPath = "destinations/"
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
}
