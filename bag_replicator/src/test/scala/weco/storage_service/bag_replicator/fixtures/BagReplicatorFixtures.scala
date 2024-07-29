package weco.storage_service.bag_replicator.fixtures

import java.util.UUID
import org.scalatest.Assertion
import weco.pekko.fixtures.Pekko
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.Queue
import weco.messaging.fixtures.worker.PekkoSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_replicator.config.ReplicatorDestinationConfig
import weco.storage_service.bag_replicator.models._
import weco.storage_service.bag_replicator.replicator.models.ReplicationSummary
import weco.storage_service.bag_replicator.replicator.s3.S3Replicator
import weco.storage_service.bag_replicator.services.BagReplicatorWorker
import weco.storage_service.fixtures.OperationFixtures
import weco.storage_service.ingests.models.{
  AmazonS3StorageProvider,
  StorageProvider
}
import weco.storage_service.storage.models.IngestStepResult
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.listing.s3.S3ObjectListing
import weco.storage.locking.memory.{MemoryLockDao, MemoryLockDaoFixtures}
import weco.storage.locking.{LockDao, LockingService}
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.transfer.fixtures.S3TransferManagerFixtures

import scala.concurrent.duration._
import scala.util.Try

trait BagReplicatorFixtures
    extends Pekko
    with OperationFixtures
    with PekkoSQSWorkerFixtures
    with MemoryLockDaoFixtures
    with S3TransferManagerFixtures {

  type ReplicatorLockingService =
    LockingService[
      IngestStepResult[ReplicationSummary[S3ObjectLocationPrefix]],
      Try,
      LockDao[String, UUID]
    ]

  def createLockingServiceWith(
    lockServiceDao: LockDao[String, UUID]
  ): ReplicatorLockingService =
    new ReplicatorLockingService {
      override implicit val lockDao: LockDao[String, UUID] =
        lockServiceDao

      override protected def createContextId(): lockDao.ContextId =
        UUID.randomUUID()
    }

  def createLockingService: ReplicatorLockingService =
    createLockingServiceWith(
      lockServiceDao = new MemoryLockDao[String, UUID] {}
    )

  def withBagReplicatorWorker[R](
    queue: Queue = dummyQueue,
    bucket: Bucket,
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender = new MemoryMessageSender(),
    lockServiceDao: LockDao[String, UUID] = new MemoryLockDao[String, UUID] {},
    stepName: String = createStepName,
    replicaType: ReplicaType = chooseFrom(PrimaryReplica, SecondaryReplica),
    visibilityTimeout: Duration = 5.seconds
  )(
    testWith: TestWith[
      BagReplicatorWorker[
        String,
        String,
        S3ObjectLocation,
        S3ObjectLocation,
        S3ObjectLocationPrefix
      ],
      R
    ]
  ): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)

      implicit val metrics: MemoryMetrics = new MemoryMetrics()

      val lockingService = createLockingServiceWith(lockServiceDao)

      val replicatorDestinationConfig =
        createReplicatorDestinationConfigWith(
          bucket = bucket,
          provider = AmazonS3StorageProvider,
          replicaType = replicaType
        )

      val replicator = new S3Replicator()

      val service = new BagReplicatorWorker(
        config = createPekkoSQSWorkerConfig(queue),
        ingestUpdater = ingestUpdater,
        outgoingPublisher = outgoingPublisher,
        lockingService = lockingService,
        destinationConfig = replicatorDestinationConfig,
        replicator = replicator,
        visibilityTimeout = visibilityTimeout
      )

      service.run()

      testWith(service)
    }

  def createReplicatorDestinationConfigWith(
    bucket: Bucket,
    provider: StorageProvider = createProvider,
    replicaType: ReplicaType = chooseFrom(PrimaryReplica, SecondaryReplica)
  ): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace = bucket.name,
      provider = provider,
      replicaType = replicaType
    )

  private val listing = new S3ObjectListing()

  def verifyObjectsCopied(
    srcPrefix: S3ObjectLocationPrefix,
    dstPrefix: S3ObjectLocationPrefix
  ): Assertion = {
    val sourceItems = listing.list(srcPrefix).value
    val sourceKeyEtags = sourceItems.map {
      _.eTag()
    }

    val destinationItems = listing.list(dstPrefix).value
    val destinationKeyEtags = destinationItems.map {
      _.eTag()
    }

    destinationKeyEtags should contain theSameElementsAs sourceKeyEtags
  }
}
