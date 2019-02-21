package uk.ac.wellcome.platform.archive.bags.async.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.bags.async.services.{BagsWorkerService, StorageManifestService}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture extends Akka with SQS with UpdateStoredManifestFixture {
  def withWorkerService[R](queue: Queue, table: Table, bucket: Bucket, progressTopic: Topic)(testWith: TestWith[BagsWorkerService, R]): R =
    withUpdateStoredManifestService(table, bucket, progressTopic) { updateStoredManifestService =>
      withActorSystem { implicit actorSystem =>
        withSQSStream[NotificationMessage, R](queue) { sqsStream =>
          val storageManifestService = new StorageManifestService(
            s3Client = s3Client
          )

          val service = new BagsWorkerService(
            sqsStream = sqsStream,
            storageManifestService = storageManifestService,
            updateStoredManifestService = updateStoredManifestService
          )

          service.run()

          testWith(service)
        }
      }
    }
}
