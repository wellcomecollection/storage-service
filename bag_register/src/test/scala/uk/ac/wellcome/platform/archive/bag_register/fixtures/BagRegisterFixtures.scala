package uk.ac.wellcome.platform.archive.bag_register.fixtures

import org.scalatest.Assertion
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.messaging.fixtures.worker.AlpakkaSQSWorkerFixtures
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bag_register.services.{BagRegisterWorker, Register}
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.{MonitoringClientFixture, OperationFixtures, RandomThings, StorageManifestVHSFixture}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestStatusUpdate}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.services.{StorageManifestService, StorageManifestVHS}
import uk.ac.wellcome.storage.StorageBackend
import uk.ac.wellcome.storage.memory.MemoryStorageBackend

trait BagRegisterFixtures
    extends RandomThings
    with AlpakkaSQSWorkerFixtures
    with OperationFixtures
    with StorageManifestVHSFixture
    with MonitoringClientFixture
    with IngestUpdateAssertions {

  type Fixtures = (BagRegisterWorker[String, String], StorageManifestVHS, MemoryMessageSender, MemoryMessageSender, QueuePair)

  def withBagRegisterWorker[R](
    storageBackend: StorageBackend = new MemoryStorageBackend(),
    vhs: StorageManifestVHS = createStorageManifestVHS()
  )(
    testWith: TestWith[Fixtures, R]): R =
    withActorSystem { implicit actorSystem =>
      withMonitoringClient { implicit monitoringClient =>
        val ingests = createMessageSender
        val outgoing = createMessageSender

        withLocalSqsQueueAndDlq { queuePair =>
          val storageManifestService =
            new StorageManifestService()(storageBackend)

          val register = new Register(
            storageManifestService = storageManifestService,
            storageManifestVHS = vhs
          )

          val ingestUpdater = new IngestUpdater[String](
            stepName = "register",
            messageSender = ingests
          )
          val outgoingPublisher = new OutgoingPublisher[String](
            operationName = "register",
            messageSender = outgoing
          )

          val service = new BagRegisterWorker(
            alpakkaSQSWorkerConfig =
              createAlpakkaSQSWorkerConfig(queuePair.queue),
            ingestUpdater = ingestUpdater,
            outgoingPublisher = outgoingPublisher,
            register = register
          )

          service.run()

          testWith((service, vhs, ingests, outgoing, queuePair))
        }
      }
    }

  def assertBagRegisterSucceeded(ingests: MemoryMessageSender)(ingestId: IngestID,
                                 bagId: BagId): Assertion =
    assertReceivesIngestUpdates(ingests)(ingestId) { ingestUpdates =>
      ingestUpdates.size shouldBe 2

      val ingestStart = ingestUpdates.head
      ingestStart.events.head.description shouldBe "Register started"

      val ingestCompleted =
        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
      ingestCompleted.status shouldBe Ingest.Completed
      ingestCompleted.affectedBag shouldBe Some(bagId)
      ingestCompleted.events.head.description shouldBe "Register succeeded (completed)"
    }

  def assertBagRegisterFailed(ingests: MemoryMessageSender)(ingestId: IngestID,
                              bagId: BagId): Assertion =
    assertReceivesIngestUpdates(ingests)(ingestId) { ingestUpdates =>
      ingestUpdates.size shouldBe 2

      val ingestStart = ingestUpdates.head
      ingestStart.events.head.description shouldBe "Register started"

      val ingestFailed =
        ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
      ingestFailed.status shouldBe Ingest.Failed
      ingestFailed.affectedBag shouldBe Some(bagId)
      ingestFailed.events.head.description shouldBe "Register failed"
    }
}
