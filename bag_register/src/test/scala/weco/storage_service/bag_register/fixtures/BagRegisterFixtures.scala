package weco.storage_service.bag_register.fixtures

import org.scalatest.Assertion
import weco.akka.fixtures.Akka
import weco.fixtures.TestWith
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.{Queue, QueuePair}
import weco.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import weco.messaging.memory.MemoryMessageSender
import weco.monitoring.memory.MemoryMetrics
import weco.storage_service.bag_register.services.{
  BagRegisterWorker,
  Register,
  S3StorageManifestService
}
import weco.storage_service.bag_tracker.fixtures.{
  BagTrackerFixtures,
  StorageManifestDaoFixture
}
import weco.storage_service.bag_tracker.storage.StorageManifestDao
import weco.storage_service.bagit.services.s3.S3BagReader
import weco.storage_service.fixtures._
import weco.storage_service.fixtures.s3.S3BagBuilder
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage_service.ingests.fixtures.IngestUpdateAssertions
import weco.storage_service.ingests.models.{Ingest, IngestStatusUpdate}
import weco.storage.store.fixtures.StringNamespaceFixtures

import scala.concurrent.ExecutionContext.Implicits.global

trait BagRegisterFixtures
    extends StorageRandomGenerators
    with Akka
    with AlpakkaSQSWorkerFixtures
    with OperationFixtures
    with StorageManifestDaoFixture
    with IngestUpdateAssertions
    with BagTrackerFixtures
    with StringNamespaceFixtures
    with S3BagBuilder {

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
      implicit val metrics: MemoryMetrics = new MemoryMetrics()

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

  def assertBagRegisterSucceeded(
    ingestsMessageSender: MemoryMessageSender
  ): Assertion =
    assertTopicReceivesIngestUpdates(ingestsMessageSender) { ingestUpdates =>
      ingestUpdates.size shouldBe 2

      val ingestStart = ingestUpdates.head
      ingestStart.events.head.description shouldBe "Register started"

      val ingestCompleted =
        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
      ingestCompleted.status shouldBe Ingest.Succeeded
      ingestCompleted.events.head.description shouldBe "Register succeeded (completed)"
    }

  def assertBagRegisterFailed(
    ingestsMessageSender: MemoryMessageSender
  ): Assertion =
    assertTopicReceivesIngestUpdates(ingestsMessageSender) { ingestUpdates =>
      ingestUpdates.size shouldBe 2

      val ingestStart = ingestUpdates.head
      ingestStart.events.head.description shouldBe "Register started"

      val ingestFailed =
        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
      ingestFailed.status shouldBe Ingest.Failed
      ingestFailed.events.head.description shouldBe "Register failed"
    }
}
