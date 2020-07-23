package uk.ac.wellcome.platform.archive.bag_register.fixtures

import org.scalatest.Assertion
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.{Queue, QueuePair}
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.messaging.sqs.SQSClientFactory
import uk.ac.wellcome.platform.archive.bag_register.services.{
  BagRegisterWorker,
  Register,
  S3StorageManifestService
}
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures._
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.s3.{NewS3StreamStore, NewS3TypedStore}

import scala.concurrent.ExecutionContext.Implicits.global

trait BagRegisterFixtures
    extends StorageRandomThings
    with Akka
    with AlpakkaSQSWorkerFixtures
    with OperationFixtures
    with StorageManifestDaoFixture
    with IngestUpdateAssertions
    with ExternalIdentifierGenerators
    with BagTrackerFixtures
    with StringNamespaceFixtures
    with StorageSpaceGenerators
    with S3BagBuilder {

  override implicit val asyncSqsClient: SqsAsyncClient =
    SQSClientFactory.createAsyncClient(
      region = "localhost",
      endpoint = "http://localhost:9324",
      accessKey = "access",
      secretKey = "secret"
    )

  type Fixtures = (
    BagRegisterWorker[String, String],
    StorageManifestDao,
    MemoryMessageSender,
    MemoryMessageSender,
    QueuePair
  )

  def withBagRegisterWorker[R](
    queue: Queue = dummyQueue,
    ingests: MemoryMessageSender = new MemoryMessageSender(),
    registrationNotifications: MemoryMessageSender = new MemoryMessageSender(),
    storageManifestDao: StorageManifestDao = createStorageManifestDao()
  )(
    testWith: TestWith[BagRegisterWorker[String, String], R]
  ): R =
    withActorSystem { implicit actorSystem =>
      withFakeMonitoringClient() { implicit monitoringClient =>
        val bagReader = new S3BagReader()

        val storageManifestService = new S3StorageManifestService()

        withBagTrackerClient(storageManifestDao) { bagTrackerClient =>
          val register = new Register(
            bagReader = bagReader,
            bagTrackerClient = bagTrackerClient,
            storageManifestService = storageManifestService
          )

          val service = new BagRegisterWorker(
            config = createAlpakkaSQSWorkerConfig(queue),
            ingestUpdater =
              createIngestUpdaterWith(ingests, stepName = "register"),
            registrationNotifications = registrationNotifications,
            register = register,
            metricsNamespace = "bag_register"
          )

          service.run()

          testWith(service)
        }
      }
    }

  def assertBagRegisterSucceeded(
    ingestId: IngestID,
    ingests: MemoryMessageSender
  ): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingests) { ingestUpdates =>
      ingestUpdates.size shouldBe 2

      val ingestStart = ingestUpdates.head
      ingestStart.events.head.description shouldBe "Register started"

      val ingestCompleted =
        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
      ingestCompleted.status shouldBe Ingest.Succeeded
      ingestCompleted.events.head.description shouldBe "Register succeeded (completed)"
    }

  def assertBagRegisterFailed(
    ingestId: IngestID,
    ingests: MemoryMessageSender
  ): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingests) { ingestUpdates =>
      ingestUpdates.size shouldBe 2

      val ingestStart = ingestUpdates.head
      ingestStart.events.head.description shouldBe "Register started"

      val ingestFailed =
        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
      ingestFailed.status shouldBe Ingest.Failed
      ingestFailed.events.head.description shouldBe "Register failed"
    }

  def createRegisterBagWith(
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    space: StorageSpace,
    version: BagVersion,
    dataFileCount: Int = randomInt(1, 15)
  )(
    implicit bucket: Bucket
  ): (S3ObjectLocationPrefix, BagInfo) = {
    implicit val streamStore: NewS3StreamStore = new NewS3StreamStore()
    implicit val typedStore: NewS3TypedStore[String] =
      new NewS3TypedStore[String]()

    val (bagObjects, bagRoot, bagInfo) =
      createBagContentsWith(
        space = space,
        externalIdentifier = externalIdentifier,
        version = version,
        payloadFileCount = dataFileCount
      )

    uploadBagObjects(bagRoot = bagRoot, objects = bagObjects)

    (bagRoot, bagInfo)
  }
}
