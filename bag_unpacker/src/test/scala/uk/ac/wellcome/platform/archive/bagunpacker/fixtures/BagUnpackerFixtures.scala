package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import java.io.InputStream

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.BagUnpackerWorker
import uk.ac.wellcome.platform.archive.bagunpacker.services.s3.S3Unpacker
import uk.ac.wellcome.platform.archive.common.fixtures.{MonitoringClientFixture, OperationFixtures, S3BagLocationFixtures}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

import uk.ac.wellcome.json.JsonUtil._

trait BagUnpackerFixtures
    extends SQS
    with S3BagLocationFixtures
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture {

  def withBagUnpackerWorker[R](
    queue: Queue,
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    dstBucket: Bucket,
    stepName: String = randomAlphanumericWithLength()
  )(testWith: TestWith[BagUnpackerWorker[String, String, _ <: InputStream], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
      withMonitoringClient { implicit monitoringClient =>
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
    }

  def withBagUnpackerApp[R](stepName: String)(
    testWith: TestWith[(BagUnpackerWorker[String, String, _ <: InputStream],
                        Bucket,
                        Queue,
                        MemoryMessageSender,
                        MemoryMessageSender),
                       R]): R =
    withLocalS3Bucket { sourceBucket =>
      withLocalSqsQueue { queue =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()
        withBagUnpackerWorker(
          queue,
          ingests,
          outgoing,
          sourceBucket,
          stepName = stepName
        )({ bagUnpackerProcess =>
          testWith(
            (
              bagUnpackerProcess,
              sourceBucket,
              queue,
              ingests,
              outgoing
            )
          )
        })
      }
    }
}
