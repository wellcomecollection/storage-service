package uk.ac.wellcome.platform.archive.bags.async.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.{SNS, SQS}
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.bags.async.services.BagsWorkerService
import uk.ac.wellcome.platform.archive.common.fixtures.{RandomThings, StorageManifestVHSFixture}
import uk.ac.wellcome.platform.archive.common.models.ReplicationResult
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends Akka
    with RandomThings
    with SNS
    with SQS
    with StorageManifestVHSFixture {
  def withWorkerService[R](
    table: Table,
    bucket: Bucket,
    topic: Topic,
    queue: Queue = Queue("bags_queue", "arn::bags_queue"))(
    testWith: TestWith[BagsWorkerService, R]): R =
    withActorSystem { implicit actorSystem =>
      withSQSStream[NotificationMessage, R](queue) { sqsStream =>
        withStorageManifestVHS(table, bucket) { storageManifestVHS =>
          withSNSWriter(topic) { progressSnsWriter =>
            val storageManifestService = new StorageManifestService(
              s3Client = s3Client
            )

            val service = new BagsWorkerService(
              sqsStream = sqsStream,
              storageManifestService = storageManifestService,
              storageManifestVHS = storageManifestVHS,
              progressSnsWriter = progressSnsWriter
            )

            service.run()

            testWith(service)
          }
        }
      }
    }

  def createReplicationResultWith(
    accessBagLocation: BagLocation): ReplicationResult =
    ReplicationResult(
      archiveRequestId = randomUUID,
      srcBagLocation = accessBagLocation.copy(storagePrefix = Some("archive")),
      dstBagLocation = accessBagLocation
    )
}
