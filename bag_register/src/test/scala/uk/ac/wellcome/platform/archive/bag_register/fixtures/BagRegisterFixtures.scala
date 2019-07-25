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
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures._
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestID,
  IngestStatusUpdate
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

trait BagRegisterFixtures
    extends StorageRandomThings
    with Akka
    with AlpakkaSQSWorkerFixtures
    with OperationFixtures
    with StorageManifestVHSFixture
    with MonitoringClientFixture
    with IngestUpdateAssertions
    with S3BagLocationFixtures {

  type Fixtures = (BagRegisterWorker[String, String],
                   StorageManifestDao,
                   MemoryMessageSender,
                   MemoryMessageSender,
                   QueuePair)

  def withBagRegisterWorker[R](testWith: TestWith[Fixtures, R]): R =
    withActorSystem { implicit actorSystem =>
      withMonitoringClient { implicit monitoringClient =>
        val storageManifestDao = createStorageManifestDao()

        val ingests = new MemoryMessageSender()
        val outgoing = new MemoryMessageSender()

        withLocalSqsQueueAndDlq { queuePair =>
          val register = new Register(
            bagReader = new S3BagReader(),
            storageManifestDao
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
  def withBag[R](
    bucket: Bucket,
    externalIdentifier: ExternalIdentifier,
    space: StorageSpace,
    version: Int,
    dataFileCount: Int)(testWith: TestWith[(ObjectLocation, BagInfo), R]): R =
    withS3Bag(
      bucket,
      externalIdentifier = externalIdentifier,
      space = space,
      dataFileCount = dataFileCount,
      bagRootDirectory = Some(s"v$version")) {
      case (bagRoot, bagInfo) =>
        testWith((bagRoot.join(s"v$version"), bagInfo))
    }
}
