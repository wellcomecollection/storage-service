package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.monitoring.memory.MemoryMetrics
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.BagUnpackerWorker
import uk.ac.wellcome.platform.archive.bagunpacker.services.s3.S3Unpacker
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

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
        unpacker = new S3Unpacker(),
        metricsNamespace = "bag_unpacker"
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
