package uk.ac.wellcome.platform.archive.bagverifier.services

import io.circe.Encoder
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.fixtures.{
  S3BagBuilder,
  S3BagBuilderBase
}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  EnrichedBagInformationPayload
}

import scala.util.{Failure, Success, Try}

class BagVerifierWorkerTest
    extends FunSpec
    with Matchers
    with IngestUpdateAssertions
    with IntegrationPatience
    with BagVerifierFixtures
    with PayloadGenerators {

  val dataFileCount: Int = randomInt(from = 2, to = 10)

  it(
    "updates the ingest monitor and sends an outgoing notification if verification succeeds"
  ) {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { bucket =>
      val (bagRootLocation, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

      val payload = createEnrichedBagInformationPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = bagInfo.externalIdentifier
        ),
        bagRoot = bagRootLocation
      )

      withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
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

      outgoing.getMessages[EnrichedBagInformationPayload] shouldBe Seq(payload)
    }
  }

  describe("passes through the original payload, unmodified") {
    it("EnrichedBagInformationPayload") {
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { bucket =>
        val (bagRootLocation, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

        val payload = createEnrichedBagInformationPayloadWith(
          context = createPipelineContextWith(
            externalIdentifier = bagInfo.externalIdentifier
          ),
          bagRoot = bagRootLocation
        )

        withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
          _.processMessage(payload) shouldBe a[Success[_]]
        }

        outgoing.getMessages[EnrichedBagInformationPayload] shouldBe Seq(
          payload
        )
      }
    }

    it("BagInformationPayload") {
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { bucket =>
        val (bagRoot, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

        val payload = createBagRootLocationPayloadWith(
          context = createPipelineContextWith(
            externalIdentifier = bagInfo.externalIdentifier
          ),
          bagRoot = bagRoot
        )

        withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
          _.processMessage(payload) shouldBe a[Success[_]]
        }

        outgoing.getMessages[BagRootLocationPayload] shouldBe Seq(payload)
      }
    }
  }

  it("only updates the ingest monitor if verification fails") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { bucket =>
      val badBuilder = new S3BagBuilderBase {
        override protected def createPayloadManifest(
          entries: Seq[PayloadEntry]
        ): Option[String] =
          super.createPayloadManifest(
            entries.head.copy(contents = randomAlphanumeric) +: entries.tail
          )
      }

      val (bagRootLocation, bagInfo) = badBuilder.createS3BagWith(bucket)

      val payload = createEnrichedBagInformationPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = bagInfo.externalIdentifier
        ),
        bagRoot = bagRootLocation
      )

      withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
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
      val badBuilder = new S3BagBuilderBase {
        override protected def createPayloadManifest(
          entries: Seq[PayloadEntry]
        ): Option[String] =
          None
      }

      val (bagRootLocation, bagInfo) = badBuilder.createS3BagWith(bucket)

      val payload = createEnrichedBagInformationPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = bagInfo.externalIdentifier
        ),
        bagRoot = bagRootLocation
      )

      withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
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
      val (bagRootLocation, _) = S3BagBuilder.createS3BagWith(
        bucket,
        externalIdentifier = bagInfoExternalIdentifier
      )

      val payload = createEnrichedBagInformationPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = payloadExternalIdentifier
        ),
        bagRoot = bagRootLocation
      )

      withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
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
      val (bagRootLocation, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

      val payload = createEnrichedBagInformationPayloadWith(
        context = createPipelineContextWith(
          externalIdentifier = bagInfo.externalIdentifier
        ),
        bagRoot = bagRootLocation
      )

      withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
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
