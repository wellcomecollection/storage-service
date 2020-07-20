package uk.ac.wellcome.platform.archive.bagverifier.services

import java.time.Instant

import io.circe.Encoder
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.builder.BagVerifierWorkerBuilder
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.fixtures.PayloadEntry
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{AmazonS3StorageProvider, Ingest}
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.OutgoingPublisher
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryStorageLocation, ReplicaResult}
import uk.ac.wellcome.platform.archive.common.{ReplicaResultPayload, VersionedBagRootPayload}

import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class BagVerifierWorkerTest
    extends AnyFunSpec
    with Matchers
    with IngestUpdateAssertions
    with IntegrationPatience
    with BagVerifierFixtures
    with PayloadGenerators
    with S3BagBuilder {

  val dataFileCount: Int = randomInt(from = 2, to = 10)

  it(
    "updates the ingest monitor and sends an outgoing notification if verification succeeds"
  ) {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { bucket =>
      val space = createStorageSpace

      val (bagRoot, bagInfo) = createS3BagWith(bucket, space = space)

      val payload = createVersionedBagRootPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = bagInfo.externalIdentifier,
          storageSpace = space
        ),
        bagRoot = bagRoot
      )

      withStandaloneBagVerifierWorker(
        ingests,
        outgoing,
        bucket = bucket,
        stepName = "verification"
      ) {
        _.processMessage(payload) shouldBe a[Success[_]]
      }

      assertTopicReceivesIngestEvents(
        payload.ingestId,
        ingests,
        expectedDescriptions = Seq(
          "Verification started",
          "Verification succeeded"
        )
      )

      outgoing.getMessages[VersionedBagRootPayload] shouldBe Seq(payload)
    }
  }

  describe("passes through the original payload, unmodified") {
    it("VersionedBagRootPayload") {
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { bucket =>
        val space = createStorageSpace
        val (bagRoot, bagInfo) = createS3BagWith(bucket, space = space)

        val payload = createVersionedBagRootPayloadWith(
          context = createPipelineContextWith(
            externalIdentifier = bagInfo.externalIdentifier,
            storageSpace = space
          ),
          bagRoot = bagRoot
        )

        withStandaloneBagVerifierWorker(
          ingests,
          outgoing,
          bucket = bucket,
          stepName = "verification"
        ) {
          _.processMessage(payload) shouldBe a[Success[_]]
        }

        outgoing.getMessages[VersionedBagRootPayload] shouldBe Seq(
          payload
        )
      }
    }

    it("ReplicaResultPayload") {
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { bucket =>
        val space = createStorageSpace
        val (bagRoot, bagInfo) = createS3BagWith(bucket, space = space)

        val payload = ReplicaResultPayload(
          context = createPipelineContextWith(
            externalIdentifier = bagInfo.externalIdentifier,
            storageSpace = space
          ),
          replicaResult = ReplicaResult(
            originalLocation = bagRoot,
            storageLocation = PrimaryStorageLocation(
              provider = AmazonS3StorageProvider,
              prefix = bagRoot.toObjectLocationPrefix
            ),
            timestamp = Instant.now
          ),
          version = createBagVersion
        )

        withActorSystem { implicit actorSystem =>
          withFakeMonitoringClient() { implicit monitoringClient =>
            val worker = BagVerifierWorkerBuilder.buildReplicaBagVerifierWorker(
              primaryBucket = bucket.name,
              metricsNamespace = randomAlphanumeric,
              alpakkaSqsWorkerConfig =
                createAlpakkaSQSWorkerConfig(Queue("url://q", "arn://q", visibilityTimeout = 1)),
              ingestUpdater = new IngestUpdater(
                stepName = randomAlphanumeric,
                messageSender = ingests
              ),
              outgoingPublisher = new OutgoingPublisher(messageSender = outgoing)
            )

            worker.processMessage(payload) shouldBe a[Success[_]]

            outgoing.getMessages[ReplicaResultPayload] shouldBe Seq(payload)
          }
        }
      }
    }
  }

  it("only updates the ingest monitor if verification fails") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilder {
        override protected def createPayloadManifest(
          entries: Seq[PayloadEntry]
        ): Option[String] =
          super.createPayloadManifest(
            entries.head.copy(contents = randomAlphanumeric) +: entries.tail
          )
      }

      val (bagRoot, bagInfo) = badBuilder.createS3BagWith(bucket)

      val payload = createVersionedBagRootPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = bagInfo.externalIdentifier
        ),
        bagRoot = bagRoot
      )

      withStandaloneBagVerifierWorker(
        ingests,
        outgoing,
        bucket = bucket,
        stepName = "verification"
      ) {
        _.processMessage(payload) shouldBe a[Success[_]]
      }

      outgoing.messages shouldBe empty

      assertTopicReceivesIngestStatus(
        payload.ingestId,
        ingests,
        status = Ingest.Failed
      ) { events =>
        val description = events.map { _.description }.head
        description should startWith("Verification failed")
      }
    }
  }

  it("only updates the ingest monitor if it cannot perform the verification") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilder {
        override protected def createPayloadManifest(
          entries: Seq[PayloadEntry]
        ): Option[String] =
          None
      }

      val (bagRoot, bagInfo) = badBuilder.createS3BagWith(bucket)

      val payload = createVersionedBagRootPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = bagInfo.externalIdentifier
        ),
        bagRoot = bagRoot
      )

      withStandaloneBagVerifierWorker(
        ingests,
        outgoing,
        bucket = bucket,
        stepName = "verification"
      ) {
        _.processMessage(payload) shouldBe a[Success[_]]
      }

      outgoing.messages shouldBe empty

      assertTopicReceivesIngestStatus(
        payload.ingestId,
        ingests,
        status = Ingest.Failed
      ) { events =>
        val description = events.map { _.description }.head
        description should startWith("Verification failed")
      }
    }
  }

  it("includes a specific error if the bag-info.txt is incorrect") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    val externalIdentifier = randomAlphanumeric
    val bagInfoExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_bag-info")
    val payloadExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_payload")

    withLocalS3Bucket { bucket =>
      val (bagRoot, _) = createS3BagWith(
        bucket,
        externalIdentifier = bagInfoExternalIdentifier
      )

      val payload = createVersionedBagRootPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = payloadExternalIdentifier
        ),
        bagRoot = bagRoot
      )

      withStandaloneBagVerifierWorker(
        ingests,
        outgoing,
        bucket = bucket,
        stepName = "verification"
      ) {
        _.processMessage(payload) shouldBe a[Success[_]]
      }

      assertTopicReceivesIngestStatus(
        payload.ingestId,
        ingests,
        status = Ingest.Failed
      ) { events =>
        val description = events.map { _.description }.head
        description should startWith(
          "Verification failed - External identifier in bag-info.txt does not match request"
        )
      }
    }
  }

  it("sends a ingest update before it sends an outgoing message") {
    val ingests = new MemoryMessageSender()

    val outgoing = new MemoryMessageSender() {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
        Failure(new Throwable("BOOM!"))
    }

    withLocalS3Bucket { bucket =>
      val space = createStorageSpace
      val (bagRoot, bagInfo) = createS3BagWith(bucket, space = space)

      val payload = createVersionedBagRootPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = bagInfo.externalIdentifier,
          storageSpace = space
        ),
        bagRoot = bagRoot
      )

      withStandaloneBagVerifierWorker(
        ingests,
        outgoing,
        bucket = bucket,
        stepName = "verification"
      ) {
        _.processMessage(payload) shouldBe a[Failure[_]]
      }

      assertTopicReceivesIngestEvent(payload.ingestId, ingests) { events =>
        events.map {
          _.description
        } shouldBe List("Verification succeeded")
      }
    }
  }
}
