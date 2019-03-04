package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.{NotificationStreamFixture, SNS}
import uk.ac.wellcome.platform.archive.bagverifier.services.{
  BagVerifierWorkerService,
  VerifyDigestFilesService
}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.S3

trait WorkerServiceFixture
    extends NotificationStreamFixture
    with SNS
    with S3
    with Akka {
  def withWorkerService[R](
    progressTopic: Topic,
    outgoingTopic: Topic,
    queue: Queue = Queue("fixture", arn = "arn::fixture"))(
    testWith: TestWith[BagVerifierWorkerService, R]): R =
    withNotificationStream[BagRequest, R](queue) { notificationStream =>
      withMaterializer { materializer =>
        implicit val _s3Client = s3Client
        implicit val _materializer = materializer

        withSNSWriter(progressTopic) { progressSnsWriter =>
          withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
            withActorSystem { actorSystem =>
              val ec = actorSystem.dispatcher

              info(ec.getClass.getSimpleName)

              val verifyDigestFilesService = new VerifyDigestFilesService(
                storageManifestService =
                  new StorageManifestService()(ec, _s3Client),
                s3Client = s3Client,
                algorithm = MessageDigestAlgorithms.SHA_256
              )(ec, _materializer)

              val service = new BagVerifierWorkerService(
                notificationStream = notificationStream,
                verifyDigestFilesService = verifyDigestFilesService,
                progressSnsWriter = progressSnsWriter,
                outgoingSnsWriter = outgoingSnsWriter
              )(ec)

              service.run()

              testWith(service)
            }
          }
        }
      }
    }
}
