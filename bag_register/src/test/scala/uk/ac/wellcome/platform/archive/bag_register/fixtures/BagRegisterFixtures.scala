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
  StorageManifestService
}
import uk.ac.wellcome.platform.archive.bag_tracker.fixtures.BagTrackerFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.services.memory.MemoryBagReader
import uk.ac.wellcome.platform.archive.common.fixtures._
import uk.ac.wellcome.platform.archive.common.fixtures.memory.MemoryBagBuilder
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
import uk.ac.wellcome.platform.archive.common.storage.services.memory.MemorySizeFinder
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.memory.{
  MemoryStore,
  MemoryStreamStore,
  MemoryTypedStore
}
import uk.ac.wellcome.storage._

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
    with MemoryBagBuilder {

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
  )(implicit streamStore: MemoryStreamStore[MemoryLocation]): R =
    withActorSystem { implicit actorSystem =>
      withFakeMonitoringClient() { implicit monitoringClient =>
        val bagReader = new MemoryBagReader()

        // TODO: Bridging code while we split ObjectLocation.  Remove this later.
        // See https://github.com/wellcomecollection/platform/issues/4596
        implicit val underlying =
          new MemoryStore[ObjectLocation, Array[Byte]](
            initialEntries = streamStore.memoryStore.entries.map {
              case (memoryLocation, bytes) =>
                memoryLocation.toObjectLocation -> bytes
            }
          )

        implicit val memoryStore: MemoryStreamStore[ObjectLocation] =
          new MemoryStreamStore[ObjectLocation](underlying)

        val storageManifestService = new StorageManifestService(
          sizeFinder = new MemorySizeFinder[ObjectLocation](underlying),
          toIdent = identity
        )

        withBagTrackerClient(storageManifestDao) { bagTrackerClient =>
          val register = new Register(
            bagReader = bagReader,
            bagTrackerClient = bagTrackerClient,
            storageManifestService = storageManifestService,
            toPrefix = (prefix: ObjectLocationPrefix) =>
              MemoryLocationPrefix(prefix.namespace, prefix.path)
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
    implicit
    namespace: String = randomAlphanumeric,
    streamStore: MemoryStreamStore[MemoryLocation]
  ): (MemoryLocationPrefix, BagInfo) = {
    implicit val typedStore: MemoryTypedStore[MemoryLocation, String] =
      new MemoryTypedStore[MemoryLocation, String]()

    val (bagObjects, bagRoot, bagInfo) =
      createBagContentsWith(
        space = space,
        externalIdentifier = externalIdentifier,
        version = version,
        payloadFileCount = dataFileCount
      )

    uploadBagObjects(bagObjects)

    (bagRoot, bagInfo)
  }
}
