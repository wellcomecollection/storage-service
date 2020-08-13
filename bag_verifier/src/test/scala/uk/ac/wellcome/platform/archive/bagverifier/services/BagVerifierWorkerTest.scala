package uk.ac.wellcome.platform.archive.bagverifier.services

import io.circe.Encoder
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.fixtures.PayloadEntry
import uk.ac.wellcome.platform.archive.common.fixtures.azure.AzureBagBuilder
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, PrimaryS3ReplicaLocation, SecondaryAzureReplicaLocation}
import uk.ac.wellcome.platform.archive.common.{BagRootLocationPayload, ReplicaCompletePayload}
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.store.azure.AzureTypedStore
import uk.ac.wellcome.storage.store.s3.S3TypedStore

import scala.util.{Failure, Success, Try}

class BagVerifierWorkerTest
    extends AnyFunSpec
    with Matchers
    with IngestUpdateAssertions
    with IntegrationPatience
    with BagVerifierFixtures
    with PayloadGenerators{
val s3BagBuilder = new S3BagBuilder {}
  val azureBagBuilder = new AzureBagBuilder {}
  val dataFileCount: Int = randomInt(from = 2, to = 10)
  it(
    "updates the ingest monitor and sends an outgoing notification if verification succeeds"
  ) {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { bucket =>
      val space = createStorageSpace

      val (bagRoot, bagInfo) = s3BagBuilder.storeBagWith(space = space)(namespace = bucket, primaryBucket = bucket)

      val payload = createBagRootLocationPayloadWith(
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

      outgoing.getMessages[BagRootLocationPayload] shouldBe Seq(payload)
    }
  }

  describe("passes through the original payload, unmodified") {
    it("BagRootLocationPayload") {
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { bucket =>
        val space = createStorageSpace
        val (bagRoot, bagInfo) = s3BagBuilder.storeBagWith(space = space)(namespace = bucket, primaryBucket = bucket)

        val payload = createBagRootLocationPayloadWith(
          context = createPipelineContextWith(
            externalIdentifier = bagInfo.externalIdentifier,
            storageSpace = space
          ),
          bagRoot = bagRoot
        )

        withStandaloneBagVerifierWorker(
          outgoing = outgoing,
          bucket = bucket
        ) {
          _.processMessage(payload) shouldBe a[Success[_]]
        }

        outgoing.getMessages[BagRootLocationPayload] shouldBe Seq(payload)
      }
    }

    it("ReplicaResultPayload - s3") {
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { bucket =>
        val space = createStorageSpace
        val (bagRoot, bagInfo) = s3BagBuilder.storeBagWith(space = space)(namespace = bucket, primaryBucket = bucket)

        val payload = ReplicaCompletePayload(
          context = createPipelineContextWith(
            externalIdentifier = bagInfo.externalIdentifier,
            storageSpace = space
          ),
          srcPrefix = bagRoot,
          dstLocation = PrimaryS3ReplicaLocation(prefix = bagRoot),
          version = createBagVersion
        )

        val result =
          withS3ReplicaBagVerifierWorker(
            outgoing = outgoing,
            bucket = bucket
          ) {
            _.processMessage(payload)
          }

        result shouldBe a[Success[_]]

        outgoing.getMessages[ReplicaCompletePayload] shouldBe Seq(payload)
      }
    }

    it("ReplicaResultPayload - Azure") {
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { primaryBucket =>
      withLocalS3Bucket { srcBucket =>
        withAzureContainer {  container =>
          val space = createStorageSpace
          val (bagRoot, bagInfo) = azureBagBuilder.storeBagWith(space = space)(namespace = container, primaryBucket = primaryBucket)
          val srcBagRoot = S3ObjectLocationPrefix(srcBucket.name, bagRoot.namePrefix)
          val srcTagManifest = for {
           tagManifest<- AzureTypedStore[String].get(bagRoot.asLocation("tagmanifest-sha256.txt"))
                   _                  <- S3TypedStore[String].put(srcBagRoot.asLocation("tagmanifest-sha256.txt"))(tagManifest.identifiedT)
          } yield ()

          srcTagManifest shouldBe a[Right[_, _]]

          val payload = ReplicaCompletePayload(
            context = createPipelineContextWith(
              externalIdentifier = bagInfo.externalIdentifier,
              storageSpace = space
            ),
            srcPrefix = srcBagRoot,
            dstLocation = SecondaryAzureReplicaLocation(prefix = bagRoot),
            version = createBagVersion
          )

          val result =
            withAzureReplicaBagVerifierWorker(
              outgoing = outgoing,
              bucket = primaryBucket
            ) {
              _.processMessage(payload)
            }

          result shouldBe a[Success[_]]

          outgoing.getMessages[ReplicaCompletePayload] shouldBe Seq(payload)
        }
      }
    }
    }
  }

  describe("handling a replicated bag") {
    it("fails if it cannot find the original bag") {
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { bucket =>
        val space = createStorageSpace
        val (bagRoot, bagInfo) = s3BagBuilder.storeBagWith(space = space)(namespace = bucket, primaryBucket = bucket)

        val payload = ReplicaCompletePayload(
          context = createPipelineContextWith(
            externalIdentifier = bagInfo.externalIdentifier,
            storageSpace = space
          ),
          srcPrefix = createS3ObjectLocationPrefix,
          dstLocation = PrimaryS3ReplicaLocation(prefix = bagRoot),
          version = createBagVersion
        )

        val result =
          withS3ReplicaBagVerifierWorker(
            outgoing = outgoing,
            bucket = bucket
          ) {
            _.processMessage(payload)
          }

        result shouldBe a[Success[_]]
        result.get shouldBe a[IngestFailed[_]]

        outgoing.messages shouldBe empty
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

      val (bagRoot, bagInfo) = badBuilder.storeBagWith()(namespace = bucket, primaryBucket = bucket)

      val payload = createBagRootLocationPayloadWith(
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

      val (bagRoot, bagInfo) = badBuilder.storeBagWith()(namespace = bucket, primaryBucket = bucket)

      val payload = createBagRootLocationPayloadWith(
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
      val (bagRoot, _) = s3BagBuilder.storeBagWith(
        externalIdentifier = bagInfoExternalIdentifier
      )(namespace = bucket, primaryBucket = bucket)

      val payload = createBagRootLocationPayloadWith(
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

      outgoing.messages shouldBe empty

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
      val (bagRoot, bagInfo) = s3BagBuilder.storeBagWith( space = space)(namespace = bucket, primaryBucket = bucket)

      val payload = createBagRootLocationPayloadWith(
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
