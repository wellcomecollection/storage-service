package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.{NotificationStreamFixture, SNS}
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifierWorkerService, VerifyDigestFilesService}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture extends NotificationStreamFixture with SNS with S3 {
  def withWorkerService[R](progressTopic: Topic, ongoingTopic: Topic, queue: Queue = Queue("fixture", arn = "arn::fixture"))(testWith: TestWith[BagVerifierWorkerService, R]): R =
    withNotificationStream[BagRequest, R](queue) { notificationStream =>
      implicit val _ = s3Client

      val verifyDigestFilesService = new VerifyDigestFilesService(
        storageManifestService = new StorageManifestService(),
        s3Client = s3Client,
        algorithm = MessageDigestAlgorithms.SHA_256
      )

      withSNSWriter(progressTopic) { progressSnsWriter =>
        withSNSWriter(ongoingTopic) { ongoingSnsWriter =>
          val service = new BagVerifierWorkerService(
            notificationStream = notificationStream,
            verifyDigestFilesService = verifyDigestFilesService,
            progressSnsWriter = progressSnsWriter,
            ongoingSnsWriter = ongoingSnsWriter
          )

          service.run()

          testWith(service)
        }
      }
    }
}
