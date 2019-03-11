package uk.ac.wellcome.platform.archive.bag_register.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.NotificationStreamFixture
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bag_register.services.{BagRegisterWorker, Register}
import uk.ac.wellcome.platform.archive.common.fixtures.{OperationFixtures, RandomThings, StorageManifestVHSFixture}
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.BagLocation
import uk.ac.wellcome.platform.archive.common.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerFixture
    extends RandomThings
    with NotificationStreamFixture
    with OperationFixtures
    with StorageManifestVHSFixture {

  def withWorkerService[R](
                            userTable: Option[Table] = None,
                            userBucket: Option[Bucket] = None
                          )(testWith: TestWith[(BagRegisterWorker,
    Table,
    Bucket,
    Topic,
    Topic,
    QueuePair),
    R]): R = {

    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withLocalSqsQueueAndDlq { queuePair =>
              withNotificationStream[BagRequest, R](queuePair.queue) { stream =>
                val testTable = if (userTable.isDefined) {
                  userTable.get
                } else {
                  table
                }

                val testBucket = if (userBucket.isDefined) {
                  userBucket.get
                } else {
                  bucket
                }

                withStorageManifestVHS(testTable, testBucket) {
                  storageManifestVHS =>
                    val storageManifestService =
                      new StorageManifestService()

                    val register = new Register(
                      storageManifestService,
                      storageManifestVHS
                    )

                    withOperationNotifier(
                      "register",
                      ingestTopic = ingestTopic,
                      outgoingTopic = outgoingTopic) { notifier =>
                      withOperationReporter() { reporter =>
                        val service =
                          new BagRegisterWorker(stream, notifier, reporter, register)

                        service.run()

                        testWith(
                          (
                            service,
                            table,
                            bucket,
                            ingestTopic,
                            outgoingTopic,
                            queuePair)
                        )
                      }
                    }
                }
              }
            }
          }
        }
      }
    }
  }


  def createBagRequestWith(
    location: BagLocation
  ): BagRequest =
    BagRequest(
      requestId = randomUUID,
      bagLocation = location
    )

}
