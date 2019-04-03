package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.MetricsFixtures
import uk.ac.wellcome.messaging.fixtures.{Messaging, SQS}
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  BagUnpackerWorker,
  S3Uploader,
  Unpacker
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  OperationFixtures,
  RandomThings
}
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

trait BagUnpackerFixtures
    extends S3
    with SQS
    with RandomThings
    with Messaging
    with Akka
    with BagLocationFixtures
    with MetricsFixtures
    with OperationFixtures {

  def withFakeMonitoringClient[R]()(
    testWith: TestWith[FakeMonitoringClient, R])(
    implicit executionContext: ExecutionContext): R = {
    testWith(new FakeMonitoringClient()(executionContext))
  }

  def withBagUnpackerWorker[R](
    queue: Queue,
    ingestTopic: Topic,
    outgoingTopic: Topic,
    dstBucket: Bucket
  )(testWith: TestWith[BagUnpackerWorker, R]): R =
    withActorSystem { implicit actorSystem =>
      withIngestUpdater("unpacker", ingestTopic) { ingestUpdater =>
        withOutgoingPublisher("unpacker", outgoingTopic) { ongoingPublisher =>
          withFakeMonitoringClient() { implicit monitoringClient =>
            implicit val _asyncSqsClient = asyncSqsClient

            val bagUnpackerWorker = BagUnpackerWorker(
              alpakkaSQSWorkerConfig = AlpakkaSQSWorkerConfig("test", queue.url),
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
