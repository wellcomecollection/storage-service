package uk.ac.wellcome.platform.archive.bag_register.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.platform.archive.bag_register.services.{
  BagRegisterWorker,
  Register
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  MonitoringClientFixture,
  OperationFixtures,
  RandomThings,
  StorageManifestVHSFixture
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait BagRegisterFixtures
    extends RandomThings
    with AlpakkaSQSWorkerFixtures
    with OperationFixtures
    with StorageManifestVHSFixture
    with MonitoringClientFixture {

  type Fixtures = (BagRegisterWorker, Table, Bucket, Topic, Topic, QueuePair)

  def withBagRegisterWorkerAndBucket[R](userBucket: Bucket)(
    testWith: TestWith[Fixtures, R]): R =
    withActorSystem { implicit actorSystem =>
      withMonitoringClient { implicit monitoringClient =>
        withLocalDynamoDbTable { table =>
          withLocalSnsTopic { ingestTopic =>
            withLocalSnsTopic { outgoingTopic =>
              withLocalSqsQueueAndDlq { queuePair =>
                withLocalS3Bucket { bucket =>
                  withStorageManifestVHS(table, userBucket) {
                    storageManifestVHS =>
                      val storageManifestService =
                        new StorageManifestService()

                      val register = new Register(
                        storageManifestService,
                        storageManifestVHS
                      )
                      withIngestUpdater("register", ingestTopic) {
                        ingestUpdater =>
                          withOutgoingPublisher("register", outgoingTopic) {
                            outgoingPublisher =>
                              val service = new BagRegisterWorker(
                                alpakkaSQSWorkerConfig = AlpakkaSQSWorkerConfig(
                                  "test",
                                  queuePair.queue.url),
                                ingestUpdater = ingestUpdater,
                                outgoingPublisher = outgoingPublisher,
                                register = register
                              )

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

  def withBagRegisterWorker[R](testWith: TestWith[Fixtures, R]): R =
    withLocalS3Bucket { bucket =>
      withBagRegisterWorkerAndBucket(bucket) { fixtures =>
        testWith(fixtures)
      }
    }
}
