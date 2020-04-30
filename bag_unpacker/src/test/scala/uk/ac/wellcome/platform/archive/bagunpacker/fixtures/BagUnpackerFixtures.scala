package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.platform.archive.bagunpacker.services.BagUnpackerWorker
import uk.ac.wellcome.platform.archive.bagunpacker.services.s3.S3Unpacker
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.streaming.{InputStreamWithLength, InputStreamWithLengthAndMetadata}
import uk.ac.wellcome.storage.{Identified, ObjectLocation}

import scala.concurrent.ExecutionContext.Implicits.global

trait BagUnpackerFixtures
    extends SQS
    with OperationFixtures
    with Akka
    with AlpakkaSQSWorkerFixtures
    with S3Fixtures {

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
      withFakeMonitoringClient() { implicit monitoringClient =>
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
    }

  def withBagUnpackerApp[R](stepName: String)(
    testWith: TestWith[
      (
        BagUnpackerWorker[String, String],
        Bucket,
        Queue,
        MemoryMessageSender,
        MemoryMessageSender
      ),
      R
    ]
  ): R =
    withLocalS3Bucket { dstBucket =>
      withLocalSqsQueue { queue =>
        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()
        withBagUnpackerWorker(
          queue,
          ingests,
          outgoing,
          dstBucket,
          stepName = stepName
        )({ bagUnpackerProcess =>
          testWith(
            (
              bagUnpackerProcess,
              dstBucket,
              queue,
              ingests,
              outgoing
            )
          )
        })
      }
    }

  // TODO: Add covariance to StreamStore
  def withStreamStore[R](
    testWith: TestWith[StreamStore[ObjectLocation, InputStreamWithLength], R]
  ): R = {
    val s3StreamStore = new S3StreamStore()

    val store = new StreamStore[ObjectLocation, InputStreamWithLength] {
      override def get(location: ObjectLocation): ReadEither =
        s3StreamStore
          .get(location)
          .map { is =>
            Identified(
              is.id,
              new InputStreamWithLength(
                is.identifiedT,
                length = is.identifiedT.length
              )
            )
          }

      override def put(
        location: ObjectLocation
      )(is: InputStreamWithLength): WriteEither =
        s3StreamStore
          .put(location)(
            new InputStreamWithLengthAndMetadata(
              is,
              length = is.length,
              metadata = Map.empty
            )
          )
          .map { is =>
            is.copy(
              identifiedT = new InputStreamWithLength(
                is.identifiedT,
                length = is.identifiedT.length
              )
            )
          }
    }

    testWith(store)
  }
}
