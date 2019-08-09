package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import java.util.UUID

import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.Assertion
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.bagreplicator.services.{
  BagReplicator,
  BagReplicatorWorker
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  MonitoringClientFixture,
  OperationFixtures,
  S3BagBuilderBase
}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.locking.memory.{
  MemoryLockDao,
  MemoryLockDaoFixtures
}
import uk.ac.wellcome.storage.locking.{LockDao, LockingService}
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.transfer.s3.S3PrefixTransfer

import scala.collection.JavaConverters._
import scala.util.Try

trait BagReplicatorFixtures
    extends Akka
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with MemoryLockDaoFixtures
    with S3BagBuilderBase {

  def createLockingServiceWith(
    lockServiceDao: LockDao[String, UUID] = new MemoryLockDao[String, UUID] {}
  ): LockingService[IngestStepResult[ReplicationSummary], Try, LockDao[
    String,
    UUID
  ]] =
    new LockingService[IngestStepResult[
      ReplicationSummary
    ], Try, LockDao[String, UUID]] {
      override implicit val lockDao: LockDao[String, UUID] =
        lockServiceDao

      override protected def createContextId(): lockDao.ContextId =
        UUID.randomUUID()
    }

  def createLockingService
    : LockingService[IngestStepResult[ReplicationSummary], Try, LockDao[
      String,
      UUID
    ]] =
    createLockingServiceWith()

  def withBagReplicatorWorker[R](
    queue: Queue =
      Queue(randomAlphanumericWithLength(), randomAlphanumericWithLength()),
    bucket: Bucket = Bucket(randomAlphanumericWithLength()),
    rootPath: Option[String] = None,
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender = new MemoryMessageSender(),
    lockServiceDao: LockDao[String, UUID] = new MemoryLockDao[String, UUID] {},
    stepName: String = randomAlphanumericWithLength()
  )(
    testWith: TestWith[BagReplicatorWorker[String, String], R]
  ): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
      withMonitoringClient { implicit monitoringClient =>
        val lockingService = createLockingServiceWith(lockServiceDao)

        val replicatorDestinationConfig =
          createReplicatorDestinationConfigWith(bucket, rootPath)

        implicit val prefixTransfer: S3PrefixTransfer =
          S3PrefixTransfer()

        implicit val streamStore: S3StreamStore =
          new S3StreamStore()

        val service = new BagReplicatorWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
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

  def createReplicatorDestinationConfigWith(
    bucket: Bucket,
    rootPath: Option[String] = None
  ): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace = bucket.name,
      rootPath = rootPath
    )

  def withBagObjects[R](bucket: Bucket, objectCount: Int = 50)(
    testWith: TestWith[ObjectLocation, R]
  ): R = {
    val builder = new S3BagBuilderBase {
      override protected def getFetchEntryCount(payloadFileCount: Int): Int = 0
    }

    val (root, _) = builder.createS3BagWith(
      bucket = bucket,
      payloadFileCount = objectCount -
        Seq(
          "bag-info.txt",
          "bagit.txt",
          "manifest-sha256.txt",
          "tagmanifest-sha256.txt"
        ).size
    )

    testWith(root)
  }

  def verifyObjectsCopied(
    src: ObjectLocation,
    dst: ObjectLocation
  ): Assertion = {
    val sourceItems = getObjectSummaries(src)
    val sourceKeyEtags = sourceItems.map {
      _.getETag
    }

    val destinationItems = getObjectSummaries(dst)
    val destinationKeyEtags = destinationItems.map {
      _.getETag
    }

    destinationKeyEtags should contain theSameElementsAs sourceKeyEtags
  }

  private def getObjectSummaries(
    objectLocation: ObjectLocation
  ): List[S3ObjectSummary] =
    s3Client
      .listObjects(objectLocation.namespace, objectLocation.path)
      .getObjectSummaries
      .asScala
      .toList
}
