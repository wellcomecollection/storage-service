package uk.ac.wellcome.platform.archive.bags.async.fixtures

import java.util.UUID

import com.amazonaws.services.dynamodbv2.model._
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.{Queue, QueuePair}
import uk.ac.wellcome.messaging.sns.NotificationMessage
import uk.ac.wellcome.platform.archive.common.fixtures.{
  ArchiveMessaging,
  BagLocationFixtures
}
import uk.ac.wellcome.platform.archive.common.generators.BagInfoGenerators
import uk.ac.wellcome.platform.archive.common.models._
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagInfo,
  BagLocation
}
import uk.ac.wellcome.platform.archive.bags.async.Registrar
import uk.ac.wellcome.platform.archive.bags.async.services.{
  BagsWorkerService,
  StorageManifestService
}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.storage.fixtures.{LocalDynamoDb, S3}
import uk.ac.wellcome.platform.archive.bags.fixtures.StorageManifestVHSFixture

import scala.concurrent.ExecutionContext.Implicits.global

trait RegistrarFixtures
    extends S3
    with Akka
    with Messaging
    with ArchiveMessaging
    with BagInfoGenerators
    with BagLocationFixtures
    with LocalDynamoDb
    with StorageManifestVHSFixture
    with UpdateStoredManifestFixture {

  def withBagNotification[R](
    queue: Queue,
    storageBucket: Bucket,
    archiveRequestId: UUID = randomUUID,
    storageSpace: StorageSpace = createStorageSpace,
    bagInfo: BagInfo = createBagInfo
  )(testWith: TestWith[(BagLocation, BagLocation), R]): R =
    withBag(
      storageBucket,
      bagInfo = bagInfo,
      storageSpace = storageSpace,
      storagePrefix = "archive") { srcBagLocation =>
      val dstBagLocation = srcBagLocation.copy(storagePrefix = Some("access"))
      val replicationResult = ReplicationResult(
        archiveRequestId = archiveRequestId,
        srcBagLocation = srcBagLocation,
        dstBagLocation = dstBagLocation
      )

      sendNotificationToSQS(queue, replicationResult)
      testWith((srcBagLocation, dstBagLocation))
    }

  override def createTable(table: Table) = {
    dynamoDbClient.createTable(
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(
          new KeySchemaElement()
            .withAttributeName("id")
            .withKeyType(KeyType.HASH))
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName("id")
            .withAttributeType("S")
        )
        .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(1L)
          .withWriteCapacityUnits(1L))
    )

    table
  }

  def withWorkerService[R](
    bucket: Bucket,
    table: Table,
    queue: Queue,
    progressTopic: Topic)(testWith: TestWith[BagsWorkerService, R]): R =
    withActorSystem { implicit actorSystem =>
      withSQSStream[NotificationMessage, R](queue) { sqsStream =>
        withUpdateStoredManifestService(table, bucket, progressTopic) {
          updateStoredManifestService =>
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

  def withApp[R](hybridStoreBucket: Bucket,
                 hybridStoreTable: Table,
                 queuePair: QueuePair,
                 progressTopic: Topic)(testWith: TestWith[Registrar, R]): R =
    withActorSystem { implicit actorSystem =>
      withArchiveMessageStream[NotificationMessage, Unit, R](queuePair.queue) {
        messageStream =>
          withStorageManifestVHS(hybridStoreTable, hybridStoreBucket) {
            dataStore =>
              val registrar = new Registrar(
                snsClient = snsClient,
                progressSnsConfig = createSNSConfigWith(progressTopic),
                s3Client = s3Client,
                messageStream = messageStream,
                dataStore = dataStore
              )

              registrar.run()

              testWith(registrar)
          }
      }
    }

  def withRegistrar[R](
    testWith: TestWith[(Bucket, QueuePair, Topic, StorageManifestVHS), R])
    : R = {
    withLocalSqsQueueAndDlqAndTimeout(15) { queuePair =>
      withLocalSnsTopic { progressTopic =>
        withLocalS3Bucket { storageBucket =>
          withLocalS3Bucket { hybridStoreBucket =>
            withLocalDynamoDbTable { hybridDynamoTable =>
              withApp(
                hybridStoreBucket,
                hybridDynamoTable,
                queuePair,
                progressTopic) { _ =>
                withStorageManifestVHS(hybridDynamoTable, hybridStoreBucket) {
                  vhs =>
                    testWith(
                      (storageBucket, queuePair, progressTopic, vhs)
                    )
                }
              }
            }
          }
        }
      }
    }
  }

  def withNewRegistrar[R](
    testWith: TestWith[(Bucket, QueuePair, Topic, StorageManifestVHS), R])
    : R = {
    withLocalSqsQueueAndDlqAndTimeout(15) { queuePair =>
      withLocalSnsTopic { progressTopic =>
        withLocalS3Bucket { storageBucket =>
          withLocalS3Bucket { bucket =>
            withLocalDynamoDbTable { table =>
              withWorkerService(bucket, table, queuePair.queue, progressTopic) {
                _ =>
                  withStorageManifestVHS(table, bucket) { vhs =>
                    testWith((storageBucket, queuePair, progressTopic, vhs))
                  }
              }
            }
          }
        }
      }
    }
  }

  def withRegistrarAndBrokenVHS[R](
    testWith: TestWith[(Bucket, QueuePair, Topic, Bucket), R]): R = {
    withLocalSqsQueueAndDlqAndTimeout(5)(queuePair => {
      withLocalSnsTopic { progressTopic =>
        withLocalS3Bucket { storageBucket =>
          withLocalS3Bucket { hybridStoreBucket =>
            withApp(
              hybridStoreBucket,
              Table("does-not-exist", ""),
              queuePair,
              progressTopic) { _ =>
              testWith(
                (storageBucket, queuePair, progressTopic, hybridStoreBucket)
              )
            }
          }
        }

      }
    })
  }

}
