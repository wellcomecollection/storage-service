package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.Assertion
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.services.{
  BagReplicator,
  BagReplicatorWorker
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  MonitoringClientFixture,
  OperationFixtures
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

trait BagReplicatorFixtures
    extends Messaging
    with BagLocationFixtures
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture {

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
            withMonitoringClient { implicit monitoringClient =>
              val service = new BagReplicatorWorker(
                config = createAlpakkaSQSWorkerConfig(queue),
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

  def verifyBagCopied(src: ObjectLocation, dst: ObjectLocation): Assertion = {
    val sourceItems = getObjectSummaries(src)
    val sourceKeyEtags = sourceItems.map { _.getETag }

    val destinationItems = getObjectSummaries(dst)
    val destinationKeyEtags = destinationItems.map { _.getETag }

    destinationKeyEtags should contain theSameElementsAs sourceKeyEtags
  }

  private def getObjectSummaries(
    objectLocation: ObjectLocation): List[S3ObjectSummary] =
    s3Client
      .listObjects(objectLocation.namespace, objectLocation.key)
      .getObjectSummaries
      .asScala
      .toList

}
