package uk.ac.wellcome.platform.archive.bag_register.fixtures

import org.scalatest.Assertion
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bag_register.services.{
  BagRegisterWorker,
  Register
}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.services.memory.MemoryBagReader
import uk.ac.wellcome.platform.archive.common.fixtures._
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.{
  MemorySizeFinder,
  StorageManifestDao,
  StorageManifestService
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.memory.{MemoryStreamStore, MemoryTypedStore}

trait BagRegisterFixtures
    extends StorageRandomThings
    with Akka
    with AlpakkaSQSWorkerFixtures
    with OperationFixtures
    with StorageManifestVHSFixture
    with MonitoringClientFixture
    with IngestUpdateAssertions
    with StringNamespaceFixtures {

  type Fixtures = (BagRegisterWorker[String, String],
                   StorageManifestDao,
                   MemoryMessageSender,
                   MemoryMessageSender,
                   QueuePair)

  def withBagRegisterWorker[R](testWith: TestWith[Fixtures, R])(
    implicit streamStore: MemoryStreamStore[ObjectLocation]): R =
    withActorSystem { implicit actorSystem =>
      withMonitoringClient { implicit monitoringClient =>
        val storageManifestDao = createStorageManifestDao()

        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()

        val bagReader = new MemoryBagReader()

        val storageManifestService = new StorageManifestService(
          sizeFinder = new MemorySizeFinder(streamStore.memoryStore)
        )

        withLocalSqsQueueAndDlq { queuePair =>
          val register = new Register(
            bagReader = bagReader,
            storageManifestDao,
            storageManifestService = storageManifestService
          )

          val service = new BagRegisterWorker(
            config = createAlpakkaSQSWorkerConfig(queuePair.queue),
            ingestUpdater =
              createIngestUpdaterWith(ingests, stepName = "register"),
            outgoingPublisher = createOutgoingPublisherWith(outgoing),
            register = register
          )

          service.run()

          testWith(
            (service, storageManifestDao, ingests, outgoing, queuePair)
          )
        }
      }
    }

  def assertBagRegisterSucceeded(ingestId: IngestID,
                                 ingests: MemoryMessageSender): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingests) { ingestUpdates =>
      ingestUpdates.size shouldBe 2

      val ingestStart = ingestUpdates.head
      ingestStart.events.head.description shouldBe "Register started"

      val ingestCompleted =
        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
      ingestCompleted.status shouldBe Ingest.Completed
      ingestCompleted.events.head.description shouldBe "Register succeeded (completed)"
    }

  def assertBagRegisterFailed(ingestId: IngestID,
                              ingests: MemoryMessageSender): Assertion =
    assertTopicReceivesIngestUpdates(ingestId, ingests) { ingestUpdates =>
      ingestUpdates.size shouldBe 2

      val ingestStart = ingestUpdates.head
      ingestStart.events.head.description shouldBe "Register started"

      val ingestFailed =
        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
      ingestFailed.status shouldBe Ingest.Failed
      ingestFailed.events.head.description shouldBe "Register failed"
    }

  // The bag register inspects the paths to a bag's entries to
  // check they are in the correct format post-replicator,
  // hence the version directory.
  def withRegisterBag[R](
    externalIdentifier: ExternalIdentifier,
    space: StorageSpace,
    version: Int,
    dataFileCount: Int)(testWith: TestWith[(ObjectLocation, BagInfo), R])(
    implicit
    namespace: String,
    streamStore: MemoryStreamStore[ObjectLocation]): R = {
    implicit val typedStore: MemoryTypedStore[ObjectLocation, String] =
      new MemoryTypedStore[ObjectLocation, String]()

    val (bagObjects, bagRoot, bagInfo) =
      BagBuilder.createBagContentsWith(
        space = space,
        externalIdentifier = externalIdentifier,
        version = BagVersion(version),
        payloadFileCount = dataFileCount
      )

    BagBuilder.uploadBagObjects(bagObjects)

    testWith((bagRoot, bagInfo))
  }
}
