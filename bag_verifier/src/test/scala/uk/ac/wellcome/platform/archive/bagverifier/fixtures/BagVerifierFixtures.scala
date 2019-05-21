package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.bagverifier.services.{BagVerifier, BagVerifierWorker, S3ObjectVerifier}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagService
import uk.ac.wellcome.platform.archive.common.fixtures.{MonitoringClientFixture, OperationFixtures}
import uk.ac.wellcome.storage.fixtures.S3

trait BagVerifierFixtures
    extends AlpakkaSQSWorkerFixtures
    with SQS
    with OperationFixtures
    with MonitoringClientFixture
    with S3 {
  def withBagVerifierWorker[R](ingests: MessageSender[String],
                               outgoing: MessageSender[String],
                               queue: Queue =
                                 Queue("fixture", arn = "arn::fixture"))(
    testWith: TestWith[BagVerifierWorker[String, String], R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer(actorSystem) { implicit mat =>
          withVerifier { verifier =>
            val ingestUpdater = createIngestUpdater(
              stepName = "verification",
              messageSender = ingests
            )

            val outgoingPublisher = createOutgoingPublisher(
              operationName = "verification",
              messageSender = outgoing
            )

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

      implicit val _bagService = new BagService()
      implicit val _s3ObjectVerifier = new S3ObjectVerifier()

      val verifier = new BagVerifier()

      testWith(verifier)
    }
}
