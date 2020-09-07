package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import java.util.UUID

import org.scalatest.Assertion
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models._
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3.S3Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.services.BagReplicatorWorker
import uk.ac.wellcome.platform.archive.common.fixtures.OperationFixtures
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  AmazonS3StorageProvider,
  StorageProvider
}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.listing.s3.S3ObjectSummaryListing
import uk.ac.wellcome.storage.locking.memory.{
  MemoryLockDao,
  MemoryLockDaoFixtures
}
import uk.ac.wellcome.storage.locking.{LockDao, LockingService}
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

trait BagReplicatorFixtures
    extends Akka
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MemoryLockDaoFixtures
    with S3Fixtures {

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
    stepName: String = randomAlphanumericWithLength(),
    replicaType: ReplicaType = chooseFrom(Seq(PrimaryReplica, SecondaryReplica))
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
      withFakeMonitoringClient() { implicit monitoringClient =>
        val lockingService = createLockingServiceWith(lockServiceDao)

        val replicatorDestinationConfig =
          createReplicatorDestinationConfigWith(
            bucket = bucket,
            provider = AmazonS3StorageProvider,
            replicaType = replicaType
          )

        val replicator = new S3Replicator()

        val service = new BagReplicatorWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher,
          lockingService = lockingService,
          destinationConfig = replicatorDestinationConfig,
          replicator = replicator,
          metricsNamespace = "bag_replicator"
        )

        service.run()

        testWith(service)
      }
    }

  def createReplicatorDestinationConfigWith(
    bucket: Bucket,
    provider: StorageProvider = createProvider,
    replicaType: ReplicaType = chooseFrom(Seq(PrimaryReplica, SecondaryReplica))
  ): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace = bucket.name,
      provider = provider,
      replicaType = replicaType
    )

  private val listing = new S3ObjectSummaryListing()

  def verifyObjectsCopied(
    srcPrefix: S3ObjectLocationPrefix,
    dstPrefix: S3ObjectLocationPrefix
  ): Assertion = {
    val sourceItems = listing.list(srcPrefix).right.value
    val sourceKeyEtags = sourceItems.map {
      _.getETag
    }

    val destinationItems = listing.list(dstPrefix).right.value
    val destinationKeyEtags = destinationItems.map {
      _.getETag
    }

    destinationKeyEtags should contain theSameElementsAs sourceKeyEtags
  }
}
