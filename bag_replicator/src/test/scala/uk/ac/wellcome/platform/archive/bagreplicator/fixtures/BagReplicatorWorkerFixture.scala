package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import com.amazonaws.services.sqs.AmazonSQSAsync
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.sqsworker.alpakka.AlpakkaSQSWorkerConfig
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.services.{
  BagReplicator,
  BagReplicatorWorker
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  OperationFixtures,
  RandomThings
}
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.concurrent.ExecutionContext.Implicits.global

trait BagReplicatorWorkerFixture
    extends RandomThings
    with S3
    with Messaging
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures {
  def withFakeMonitoringClient[R](
    testWith: TestWith[FakeMonitoringClient, R]): R =
    testWith(new FakeMonitoringClient())

  def withBagReplicatorWorker[R](queue: Queue = Queue(
                                   "default_q",
                                   "arn::default_q"
                                 ),
                                 ingestTopic: Topic,
                                 outgoingTopic: Topic,
                                 config: ReplicatorDestinationConfig =
                                   createReplicatorDestinationConfigWith(
                                     Bucket(randomAlphanumeric())))(
    testWith: TestWith[BagReplicatorWorker, R]): R =
    withActorSystem { implicit actorSystem =>
      withIngestUpdater("replicating", ingestTopic) { ingestUpdater =>
        withOutgoingPublisher("replicating", outgoingTopic) {
          outgoingPublisher =>
            withFakeMonitoringClient { implicit monitoringClient =>
              implicit val asyncSQSClient: AmazonSQSAsync = asyncSqsClient
              val service = new BagReplicatorWorker(
                alpakkaSQSWorkerConfig =
                  AlpakkaSQSWorkerConfig("test", queue.url),
                bagReplicator = new BagReplicator(config),
                ingestUpdater = ingestUpdater,
                outgoingPublisher = outgoingPublisher
              )

              service.run()

              testWith(service)
            }
        }
      }
    }

  def createReplicatorDestinationConfigWith(
    bucket: Bucket): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace = bucket.name,
      rootPath = Some(randomAlphanumeric())
    )
}
