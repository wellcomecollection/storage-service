package weco.storage_service.bag_unpacker.fixtures

import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_unpacker.config.models.BagUnpackerWorkerConfig
import weco.storage_service.bag_unpacker.services.BagUnpackerWorker
import weco.storage_service.bag_unpacker.services.s3.S3Unpacker
import weco.storage_service.fixtures.OperationFixtures
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.S3ObjectLocation
import weco.storage.store.StreamStore
import weco.storage.store.s3.S3StreamStore

trait BagUnpackerFixtures
    extends SQS
    with OperationFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with S3Fixtures {

  def withBagUnpackerWorker[R](
    queue: Queue = dummyQueue,
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    dstBucket: Bucket,
    stepName: String = createStepName
  )(testWith: TestWith[BagUnpackerWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val bagUnpackerWorker = new BagUnpackerWorker(
        config = createAlpakkaSQSWorkerConfig(queue),
        bagUnpackerWorkerConfig = BagUnpackerWorkerConfig(dstBucket.name),
        ingestUpdater = ingestUpdater,
        outgoingPublisher = outgoingPublisher,
        unpacker = new S3Unpacker()
      )

      bagUnpackerWorker.run()

      testWith(bagUnpackerWorker)
    }

  def withBagUnpackerApp[R](stepName: String)(
    testWith: TestWith[
      (
        Bucket,
        Queue,
        MemoryMessageSender,
        MemoryMessageSender
      ),
      R
    ]
  ): R =
    withLocalS3Bucket { dstBucket =>
      withLocalSqsQueue() { queue =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()
        withBagUnpackerWorker(
          queue,
          ingests,
          outgoing,
          dstBucket,
          stepName = stepName
        ) { _ =>
          testWith(
            (
              dstBucket,
              queue,
              ingests,
              outgoing
            )
          )
        }
      }
    }

  def withStreamStore[R](
    testWith: TestWith[StreamStore[S3ObjectLocation], R]
  ): R =
    testWith(new S3StreamStore())
}
