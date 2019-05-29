package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import java.util.UUID

import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.Assertion
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.bagreplicator.services.{BagReplicator, BagReplicatorWorker}
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, MonitoringClientFixture, OperationFixtures}
import uk.ac.wellcome.storage.fixtures.LockingServiceFixtures
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.storage.memory.MemoryLockDao
import uk.ac.wellcome.storage.{LockDao, LockingService, ObjectLocation}

import scala.collection.JavaConverters._
import scala.util.Try

trait BagReplicatorFixtures
    extends Messaging
    with BagLocationFixtures
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with LockingServiceFixtures {

  def withBagReplicatorWorker[R](
    queue: Queue = Queue(randomAlphanumeric(), randomAlphanumeric()),
    bucket: Bucket = Bucket(randomAlphanumeric()),
    rootPath: Option[String] = None,
    ingests: MemoryMessageSender = createMessageSender,
    outgoing: MemoryMessageSender = createMessageSender,
    lockServiceDao: LockDao[String, UUID] = new MemoryLockDao[String, UUID] {},
    stepName: String = randomAlphanumeric())(
    testWith: TestWith[BagReplicatorWorker[String, String], R]
  ): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
      withMonitoringClient { implicit monitoringClient =>
        val lockingService = new LockingService[
          Result[ReplicationSummary],
          Try,
          LockDao[String, UUID]] {
          override implicit val lockDao: LockDao[String, UUID] =
            lockServiceDao
          override protected def createContextId(): lockDao.ContextId =
            UUID.randomUUID()
        }

        val replicatorDestinationConfig =
          createReplicatorDestinationConfigWith(bucket, rootPath)

        val service = new BagReplicatorWorker(
          alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
          bagReplicator = new BagReplicator(),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher,
          lockingService = lockingService,
          replicatorDestinationConfig = replicatorDestinationConfig
        )

        service.run()

        testWith(service)
      }
    }

  private def createReplicatorDestinationConfigWith(
    bucket: Bucket,
    rootPath: Option[String]
  ): ReplicatorDestinationConfig =
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
