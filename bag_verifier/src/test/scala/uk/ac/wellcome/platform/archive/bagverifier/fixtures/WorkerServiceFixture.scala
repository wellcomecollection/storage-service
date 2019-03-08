package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.{NotificationStreamFixture, SNS}
import uk.ac.wellcome.platform.archive.bagverifier.services.{
  BagVerifierWorker,
  Verifier
}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.operation.OperationNotifier
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.S3

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends NotificationStreamFixture
    with SNS
    with S3
    with Akka {
  def withWorkerService[R](
    ingestTopic: Topic,
    outgoingTopic: Topic,
    queue: Queue = Queue("fixture", arn = "arn::fixture"))(
    testWith: TestWith[BagVerifierWorker, R]): R =
    withNotificationStream[BagRequest, R](queue) { stream =>
      withMaterializer { implicit mat =>
        withSNSWriter(ingestTopic) { ingestSnsWriter =>
          withSNSWriter(outgoingTopic) { outgoingSnsWriter =>
            val verifier = new Verifier(
              storageManifestService = new StorageManifestService(),
              s3Client = s3Client,
              algorithm = MessageDigestAlgorithms.SHA_256
            )

            val operationName = "verification"

            val notifier = new OperationNotifier(
              operationName,
              outgoingSnsWriter,
              ingestSnsWriter
            )

            val service = new BagVerifierWorker(
              stream,
              verifier,
              notifier
            )

            service.run()

            testWith(service)
          }
        }
      }
    }
}
