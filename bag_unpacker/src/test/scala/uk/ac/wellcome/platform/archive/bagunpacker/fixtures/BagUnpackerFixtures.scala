package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  BagUnpackerWorker,
  S3Uploader,
  Unpacker
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  MonitoringClientFixture,
  OperationFixtures,
  RandomThings
}
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait BagUnpackerFixtures
    extends RandomThings
    with Messaging
    with BagLocationFixtures
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture {

  def withBagUnpackerWorker[R](
    queue: Queue,
    ingestTopic: Topic,
    outgoingTopic: Topic,
    dstBucket: Bucket
  )(testWith: TestWith[BagUnpackerWorker, R]): R =
    withActorSystem { implicit actorSystem =>
      withIngestUpdater("unpacker", ingestTopic) { ingestUpdater =>
        withOutgoingPublisher("unpacker", outgoingTopic) { ongoingPublisher =>
          withMonitoringClient { implicit monitoringClient =>
            val bagUnpackerWorker = BagUnpackerWorker(
              alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
              bagUnpackerWorkerConfig = BagUnpackerWorkerConfig(dstBucket.name),
              ingestUpdater = ingestUpdater,
              outgoingPublisher = ongoingPublisher,
              unpacker = Unpacker(new S3Uploader())
            )

            bagUnpackerWorker.run()

            testWith(bagUnpackerWorker)
          }
        }
      }
    }

  def withBagUnpackerApp[R](
    testWith: TestWith[(BagUnpackerWorker, Bucket, Queue, Topic, Topic), R])
    : R =
    withLocalS3Bucket { sourceBucket =>
      withLocalSqsQueue { queue =>
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withBagUnpackerWorker(
              queue,
              ingestTopic,
              outgoingTopic,
              sourceBucket
            )({ bagUnpackerProcess =>
              testWith(
                (
                  bagUnpackerProcess,
                  sourceBucket,
                  queue,
                  ingestTopic,
                  outgoingTopic
                )
              )
            })
          }
        }
      }
    }
}
