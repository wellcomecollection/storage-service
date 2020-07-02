package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.services.s3.S3BagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.services.{
  BagVerifier,
  BagVerifierWorker
}
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

import scala.concurrent.ExecutionContext.Implicits.global

trait BagVerifierFixtures
    extends AlpakkaSQSWorkerFixtures
    with SQS
    with Akka
    with OperationFixtures
    with S3Fixtures {
  def withBagVerifierWorker[R](
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    queue: Queue = dummyQueue,
    bucket: Bucket,
    stepName: String = randomAlphanumericWithLength()
  )(testWith: TestWith[BagVerifierWorker[String, String], R]): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withVerifier(bucket) { verifier =>
          val ingestUpdater =
            createIngestUpdaterWith(ingests, stepName = stepName)

          val outgoingPublisher = createOutgoingPublisherWith(outgoing)

          val service = new BagVerifierWorker(
            config = createAlpakkaSQSWorkerConfig(queue),
            ingestUpdater = ingestUpdater,
            outgoingPublisher = outgoingPublisher,
            verifier = verifier,
            metricsNamespace = "bag_verifier"
          )

          service.run()

          testWith(service)
        }
      }
    }

  def withVerifier[R](bucket: Bucket)(
    testWith: TestWith[BagVerifier[S3ObjectLocation, S3ObjectLocationPrefix], R]
  ): R =
    testWith(
      new S3BagVerifier(primaryBucket = bucket.name)
    )
}
