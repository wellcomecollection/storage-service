package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.services.{
  BagVerifier,
  BagVerifierWorker
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.storage.{
  ObjectLocation,
  ObjectLocationPrefix,
  S3ObjectLocation,
  S3ObjectLocationPrefix
}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing

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
    withMaterializer { implicit mat =>
      implicit val _bagReader
        : BagReader[S3ObjectLocation, S3ObjectLocationPrefix] =
        new S3BagReader()

      implicit val s3FixityChecker: S3FixityChecker =
        new S3FixityChecker()

      implicit val _s3Resolvable: S3Resolvable =
        new S3Resolvable()

      implicit val listing: S3ObjectLocationListing = S3ObjectLocationListing()

      val verifier = new BagVerifier[S3ObjectLocation, S3ObjectLocationPrefix](
        namespace = bucket.name,
        toLocation =
          (location: ObjectLocation) => S3ObjectLocation(location),
        toPrefix =
          (prefix: ObjectLocationPrefix) => S3ObjectLocationPrefix(prefix)
      )

      testWith(verifier)
    }
}
