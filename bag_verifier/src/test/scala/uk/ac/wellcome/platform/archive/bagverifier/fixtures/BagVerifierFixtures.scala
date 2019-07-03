package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, BagVerifierWorker}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagDao
import uk.ac.wellcome.platform.archive.common.fixtures.{MonitoringClientFixture, OperationFixtures}
import uk.ac.wellcome.platform.archive.common.storage.services.{S3ObjectVerifier, S3Resolvable}
import uk.ac.wellcome.storage.fixtures.S3Fixtures

trait BagVerifierFixtures
    extends AlpakkaSQSWorkerFixtures
    with SQS
    with Akka
    with OperationFixtures
    with MonitoringClientFixture
    with S3Fixtures {
  def withBagVerifierWorker[R](
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    queue: Queue = Queue("fixture", arn = "arn::fixture"),
    stepName: String = randomAlphanumericWithLength())(
    testWith: TestWith[BagVerifierWorker[String, String], R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer(actorSystem) { implicit mat =>
          withVerifier { verifier =>
            val ingestUpdater =
              createIngestUpdaterWith(ingests, stepName = stepName)
            val outgoingPublisher = createOutgoingPublisherWith(outgoing)
            val service = new BagVerifierWorker(
              alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
              ingestUpdater = ingestUpdater,
              outgoingPublisher = outgoingPublisher,
              verifier = verifier
            )

            service.run()

            testWith(service)
          }
        }
      }
    }

  def withVerifier[R](testWith: TestWith[BagVerifier, R]): R =
    withMaterializer { implicit mat =>
      implicit val _bagService = new BagDao()
      implicit val _s3ObjectVerifier = new S3ObjectVerifier()
      implicit val _s3Resolvable = new S3Resolvable()

      val verifier = new BagVerifier()

      testWith(verifier)
    }
}
