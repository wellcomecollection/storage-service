package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.services.{
  BagVerifier,
  BagVerifierWorker
}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.storage.services.S3Resolvable
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.platform.archive.common.verify.s3.S3ObjectVerifier
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
    queue: Queue = Queue("fixture", arn = "arn::fixture"),
    stepName: String = randomAlphanumericWithLength()
  )(testWith: TestWith[BagVerifierWorker[String, String], R]): R =
    withFakeMonitoringClient() { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withVerifier { verifier =>
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

  def withVerifier[R](testWith: TestWith[BagVerifier, R]): R =
    withMaterializer { implicit mat =>
      implicit val _bagReader: BagReader[_] =
        new S3BagReader()

      implicit val _s3ObjectVerifier: S3ObjectVerifier =
        new S3ObjectVerifier()

      implicit val _s3Resolvable: S3Resolvable =
        new S3Resolvable()

      implicit val listing: S3ObjectLocationListing = S3ObjectLocationListing()

      val verifier = new BagVerifier()

      testWith(verifier)
    }
}
