package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  BagUnpackerWorker,
  S3Uploader,
  Unpacker
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  MonitoringClientFixture,
  OperationFixtures
}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

trait BagUnpackerFixtures
    extends SQS
    with BagLocationFixtures
    with Akka
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture {

  def withBagUnpackerWorker[R](
    queue: Queue,
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    dstBucket: Bucket,
    stepName: String = randomAlphanumericWithLength()
  )(testWith: TestWith[BagUnpackerWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
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

  def withBagUnpackerApp[R](stepName: String)(
    testWith: TestWith[(BagUnpackerWorker[String, String],
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
