package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.{BagUnpackerWorker, S3Uploader, Unpacker}
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, MonitoringClientFixture, OperationFixtures}
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait BagUnpackerFixtures
    extends BagLocationFixtures
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with S3 {

  def withBagUnpackerWorker[R](
    queue: Queue,
    ingests: MessageSender[String],
    outgoing: MessageSender[String],
    dstBucket: Bucket
  )(testWith: TestWith[BagUnpackerWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdater(
        stepName = "unpacker",
        messageSender = ingests
      )

      val outgoingPublisher = createOutgoingPublisher(
        operationName = "unpacker",
        messageSender = outgoing
      )

      withMonitoringClient { implicit monitoringClient =>
        val bagUnpackerWorker = BagUnpackerWorker(
          alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
          bagUnpackerWorkerConfig = BagUnpackerWorkerConfig(dstBucket.name),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher,
          unpacker = Unpacker(new S3Uploader())
        )

        bagUnpackerWorker.run()

        testWith(bagUnpackerWorker)
      }
    }

  def withBagUnpackerApp[R](
    testWith: TestWith[(BagUnpackerWorker[String, String], Bucket, Queue, MemoryMessageSender, MemoryMessageSender), R])
    : R =
    withLocalS3Bucket { sourceBucket =>
      withLocalSqsQueue { queue =>
        val ingests = createMessageSender
        val outgoing = createMessageSender
        withBagUnpackerWorker(
          queue,
          ingests,
          outgoing,
          sourceBucket
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
