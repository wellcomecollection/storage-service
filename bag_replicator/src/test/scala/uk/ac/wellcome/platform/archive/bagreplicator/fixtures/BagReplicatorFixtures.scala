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
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.bagreplicator.services.{
  BagReplicator,
  BagReplicatorWorker
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  MonitoringClientFixture,
  OperationFixtures
}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.listing.s3.S3ObjectSummaryListing
import uk.ac.wellcome.storage.locking.memory.{
  MemoryLockDao,
  MemoryLockDaoFixtures
}
import uk.ac.wellcome.storage.locking.{LockDao, LockingService}
import uk.ac.wellcome.storage.transfer.s3.S3PrefixTransfer

import scala.util.Try

trait BagReplicatorFixtures
    extends Akka
    with OperationFixtures
    with AlpakkaSQSWorkerFixtures
    with MonitoringClientFixture
    with MemoryLockDaoFixtures
    with S3Fixtures {

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
        val lockingService = new LockingService[IngestStepResult[
          ReplicationSummary
        ], Try, LockDao[String, UUID]] {
          override implicit val lockDao: LockDao[String, UUID] =
            lockServiceDao

          override protected def createContextId(): lockDao.ContextId =
            UUID.randomUUID()
        }

        val replicatorDestinationConfig =
          createReplicatorDestinationConfigWith(bucket, rootPath)

        implicit val prefixTransfer: S3PrefixTransfer =
          S3PrefixTransfer()

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

  private def createReplicatorDestinationConfigWith(
    bucket: Bucket,
    rootPath: Option[String]
  ): ReplicatorDestinationConfig =
    ReplicatorDestinationConfig(
      namespace = bucket.name,
      rootPath = rootPath
    )

  private val listing = new S3ObjectSummaryListing()

  def verifyObjectsCopied(
    src: ObjectLocation,
    dst: ObjectLocation
  ): Assertion = {
    val sourceItems = listing.list(src.asPrefix).right.value
    val sourceKeyEtags = sourceItems.map {
      _.getETag
    }

    val destinationItems = listing.list(dst.asPrefix).right.value
    val destinationKeyEtags = destinationItems.map {
      _.getETag
    }

    destinationKeyEtags should contain theSameElementsAs sourceKeyEtags
  }
}
