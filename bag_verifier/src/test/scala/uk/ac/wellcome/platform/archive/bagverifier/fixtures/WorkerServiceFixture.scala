package uk.ac.wellcome.platform.archive.bagverifier.fixtures

import org.apache.commons.codec.digest.MessageDigestAlgorithms
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.platform.archive.bagverifier.services.{
  BagVerifierWorker,
  Verifier
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  MonitoringClientFixture,
  OperationFixtures
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestService

import scala.concurrent.ExecutionContext.Implicits.global

trait WorkerServiceFixture
    extends AlpakkaSQSWorkerFixtures
    with SQS
    with OperationFixtures
    with MonitoringClientFixture {
  def withBagVerifierWorker[R](ingestTopic: Topic,
                               outgoingTopic: Topic,
                               queue: Queue =
                                 Queue("fixture", arn = "arn::fixture"))(
    testWith: TestWith[BagVerifierWorker, R]): R =
    withMonitoringClient { implicit monitoringClient =>
      withActorSystem { implicit actorSystem =>
        withMaterializer(actorSystem) { implicit materializer =>
          implicit val _asyncSqsClient = asyncSqsClient
          val verifier = new Verifier(
            storageManifestService = new StorageManifestService(),
            s3Client = s3Client,
            algorithm = MessageDigestAlgorithms.SHA_256
          )
          withIngestUpdater("verification", ingestTopic) { ingestUpdater =>
            withOutgoingPublisher("verification", outgoingTopic) {
              outgoingPublisher =>
                val service = new BagVerifierWorker(
                  alpakkaSQSWorkerConfig =
                    AlpakkaSQSWorkerConfig("test", queue.url),
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
    }
}
