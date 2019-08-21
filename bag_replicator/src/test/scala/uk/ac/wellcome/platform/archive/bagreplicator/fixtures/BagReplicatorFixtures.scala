package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import java.util.UUID

import org.scalatest.Assertion
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagreplicator.bags.BagReplicator
import uk.ac.wellcome.platform.archive.bagreplicator.bags.models.{
  BagReplicationSummary,
}
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3.S3Replicator
import uk.ac.wellcome.platform.archive.bagreplicator.services.BagReplicatorWorker
import uk.ac.wellcome.platform.archive.common.fixtures.{
  MonitoringClientFixture,
  OperationFixtures
}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.listing.s3.S3ObjectSummaryListing
import uk.ac.wellcome.storage.locking.memory.{
  MemoryLockDao,
  MemoryLockDaoFixtures
}
import uk.ac.wellcome.storage.locking.{LockDao, LockingService}
import uk.ac.wellcome.storage.store.s3.S3StreamStore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait BagReplicatorFixtures
    extends Akka
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with MemoryLockDaoFixtures
    with S3Fixtures {

  type ReplicatorLockingService =
    LockingService[IngestStepResult[
      BagReplicationSummary[_]
    ], Future, LockDao[String, UUID]]

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
    queue: Queue =
      Queue(randomAlphanumericWithLength(), randomAlphanumericWithLength()),
    bucket: Bucket,
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    outgoing: MemoryMessageSender = new MemoryMessageSender(),
    lockServiceDao: LockDao[String, UUID] = new MemoryLockDao[String, UUID] {},
    stepName: String = randomAlphanumericWithLength()
  )(
    testWith: TestWith[
      BagReplicatorWorker[String, String],
      R
    ]
  ): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = createIngestUpdaterWith(ingests, stepName = stepName)
      val outgoingPublisher = createOutgoingPublisherWith(outgoing)
      withMonitoringClient { implicit monitoringClient =>
        val lockingService = createLockingServiceWith(lockServiceDao)

        val replicatorDestinationConfig =
          createReplicatorDestinationConfigWith(bucket)

        implicit val s3StreamStore: S3StreamStore =
          new S3StreamStore()

        val replicator = new S3Replicator()

        val bagReplicator =
          new BagReplicator(replicator)

        val service = new BagReplicatorWorker(
          config = createAlpakkaSQSWorkerConfig(queue),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher,
          lockingService = lockingService,
          replicatorDestinationConfig = replicatorDestinationConfig,
          bagReplicator = bagReplicator
        )

        service.run()

        testWith(service)
      }
    }

  def createReplicatorDestinationConfigWith(
    bucket: Bucket
  ): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace = bucket.name
    )

  private val listing = new S3ObjectSummaryListing()

  def verifyObjectsCopied(
    srcPrefix: ObjectLocationPrefix,
    dstPrefix: ObjectLocationPrefix
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
