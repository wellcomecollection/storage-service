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

  private val defaultQueue = Queue(
    url = "default_q",
    arn = "arn::default_q"
  )

  def withBagReplicatorWorker[R](
    ingestTopic: Topic,
    outgoingTopic: Topic)(
    testWith: TestWith[BagReplicatorWorker, R]
  ): R =
    withLocalS3Bucket { bucket =>
      val config = createReplicatorDestinationConfigWith(bucket)
      withBagReplicatorWorker(defaultQueue, ingestTopic, outgoingTopic, config) { worker =>
        testWith(worker)
      }
    }

  def withBagReplicatorWorker[R](bucket: Bucket)(testWith: TestWith[BagReplicatorWorker, R]): R = {
    val config = createReplicatorDestinationConfigWith(bucket)
    withBagReplicatorWorker(config) { worker =>
      testWith(worker)
    }
  }

  def withBagReplicatorWorker[R](
    config: ReplicatorDestinationConfig
  )(testWith: TestWith[BagReplicatorWorker, R]): R =
    withLocalSnsTopic { topic =>
      withBagReplicatorWorker(defaultQueue, topic, topic, config) { worker =>
        testWith(worker)
      }
    }

  def withBagReplicatorWorker[R](
    ingestTopic: Topic,
    outgoingTopic: Topic,
    bucket: Bucket)(
    testWith: TestWith[BagReplicatorWorker, R]
  ): R = {
    val config = createReplicatorDestinationConfigWith(bucket)
    withBagReplicatorWorker(defaultQueue, ingestTopic, outgoingTopic, config) { worker =>
      testWith(worker)
    }
  }

  def withBagReplicatorWorker[R](
    queue: Queue,
    ingestTopic: Topic,
    outgoingTopic: Topic,
    config: ReplicatorDestinationConfig)(
    testWith: TestWith[BagReplicatorWorker, R]): R =
    withActorSystem { implicit actorSystem =>
      withIngestUpdater("replicating", ingestTopic) { ingestUpdater =>
        withOutgoingPublisher("replicating", outgoingTopic) {
          outgoingPublisher =>
            withMonitoringClient { implicit monitoringClient =>
              val service = new BagReplicatorWorker(
                alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
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
    bucket: Bucket,
    rootPath: Option[String] = Some(randomAlphanumeric())): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace = bucket.name,
      rootPath = rootPath
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
