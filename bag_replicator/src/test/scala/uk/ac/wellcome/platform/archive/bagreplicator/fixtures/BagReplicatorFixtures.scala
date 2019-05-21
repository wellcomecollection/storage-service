package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import java.util.UUID

import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.Assertion
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.MessageSender
import uk.ac.wellcome.messaging.fixtures.Messaging
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.messaging.worker.models.Result
import uk.ac.wellcome.platform.archive.bagreplicator.config.ReplicatorDestinationConfig
import uk.ac.wellcome.platform.archive.bagreplicator.models.ReplicationSummary
import uk.ac.wellcome.platform.archive.bagreplicator.services.{BagReplicator, BagReplicatorWorker}
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, MonitoringClientFixture, OperationFixtures}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
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

  private val defaultQueue = Queue(
    url = "default_q",
    arn = "arn::default_q"
  )

  def withBagReplicatorWorker[R](ingests: MemoryMessageSender, outgoing: MemoryMessageSender)(
    testWith: TestWith[BagReplicatorWorker[String, String], R]
  ): R =
    withLocalS3Bucket { bucket =>
      val config = createReplicatorDestinationConfigWith(bucket)
      withBagReplicatorWorker(defaultQueue, ingests, outgoing, config) {
        worker =>
          testWith(worker)
      }
    }

  def withBagReplicatorWorker[R](ingests: MemoryMessageSender,
                                 outgoing: MemoryMessageSender,
                                 lockServiceDao: LockDao[String, UUID])(
    testWith: TestWith[BagReplicatorWorker[String, String], R]
  ): R =
    withLocalS3Bucket { bucket =>
      val config = createReplicatorDestinationConfigWith(bucket)
      withBagReplicatorWorker(
        defaultQueue,
        ingests,
        outgoing,
        config,
        lockServiceDao) { worker =>
        testWith(worker)
      }
    }

  def withBagReplicatorWorker[R](lockServiceDao: LockDao[String, UUID])(
    testWith: TestWith[BagReplicatorWorker[String, String], R]
  ): R =
    withLocalS3Bucket { bucket =>
      val config = createReplicatorDestinationConfigWith(bucket)
      withBagReplicatorWorker(
        defaultQueue,
        createMessageSender,
        createMessageSender,
        config,
        lockServiceDao) { worker =>
        testWith(worker)
      }
    }

  def withBagReplicatorWorker[R](bucket: Bucket)(
    testWith: TestWith[BagReplicatorWorker[String, String], R]): R = {
    val config = createReplicatorDestinationConfigWith(bucket)
    withBagReplicatorWorker(config) { worker =>
      testWith(worker)
    }
  }

  def withBagReplicatorWorker[R](
    config: ReplicatorDestinationConfig
  )(testWith: TestWith[BagReplicatorWorker[String, String], R]): R = {
    val messageSender = new MemoryMessageSender(
      destination = randomAlphanumeric(),
      subject = randomAlphanumeric()
    )

    withBagReplicatorWorker(defaultQueue, messageSender, messageSender, config) { worker =>
      testWith(worker)
    }
  }

  def withBagReplicatorWorker[R](ingests: MessageSender[String],
                                 outgoing: MessageSender[String],
                                 bucket: Bucket)(
    testWith: TestWith[BagReplicatorWorker[String, String], R]
  ): R = {
    val config = createReplicatorDestinationConfigWith(bucket)
    withBagReplicatorWorker(defaultQueue, ingests, outgoing, config) {
      worker =>
        testWith(worker)
    }
  }

  def withBagReplicatorWorker[R](
    queue: Queue,
    ingests: MessageSender[String],
    outgoing: MessageSender[String],
    config: ReplicatorDestinationConfig,
    lockServiceDao: LockDao[String, UUID] = new MemoryLockDao[String, UUID] {})(
    testWith: TestWith[BagReplicatorWorker[String, String], R]): R =
    withActorSystem { implicit actorSystem =>
      val ingestUpdater = new IngestUpdater[String](
        stepName = "replicating",
        messageSender = ingests
      )

      val outgoingPublisher = new OutgoingPublisher[String](
        operationName = "replicating",
        messageSender = outgoing
      )

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

        val service = new BagReplicatorWorker(
          alpakkaSQSWorkerConfig = createAlpakkaSQSWorkerConfig(queue),
          bagReplicator = new BagReplicator(),
          ingestUpdater = ingestUpdater,
          outgoingPublisher = outgoingPublisher,
          lockingService = lockingService,
          replicatorDestinationConfig = config
        )

        service.run()

        testWith(service)
      }
    }

  def createReplicatorDestinationConfigWith(bucket: Bucket,
                                            rootPath: Option[String] = Some(
                                              randomAlphanumeric()))
    : ReplicatorDestinationConfig =
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
