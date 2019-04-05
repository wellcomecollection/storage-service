package uk.ac.wellcome.platform.archive.bag_register.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.NotificationStreamFixture
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.bag_register.services.{
  BagRegisterWorker,
  Register
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  OperationFixtures,
  RandomThings,
  StorageManifestVHSFixture
}
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerFixture
    extends RandomThings
    with NotificationStreamFixture
    with OperationFixtures
    with StorageManifestVHSFixture {

  def withBagRegisterWorker[R](
    userBucket: Option[Bucket] = None
  )(testWith: TestWith[(BagRegisterWorker,
                        Table,
                        Bucket,
                        Topic,
                        Topic,
                        QueuePair),
                       R]): R =
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withLocalSqsQueueAndDlq { queuePair =>
              withNotificationStream[BagRequest, R](queuePair.queue) { stream =>
                val testBucket = if (userBucket.isDefined) {
                  userBucket.get
                } else {
                  bucket
                }

                withStorageManifestVHS(table, testBucket) {
                  storageManifestVHS =>
                    val storageManifestService =
                      new StorageManifestService()

                    val register = new Register(
                      storageManifestService,
                      storageManifestVHS
                    )
                    withIngestUpdater("register", ingestTopic) {
                      ingestNotifier =>
                        withOutgoingPublisher("register", outgoingTopic) {
                          outgoingNotifier =>
                            withOperationReporter() { reporter =>
                              val service =
                                new BagRegisterWorker(
                                  stream,
                                  ingestNotifier,
                                  outgoingNotifier,
                                  reporter,
                                  register)

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
}
